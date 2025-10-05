/*
 generadores_coordinador.c
 Compilación: gcc generadores_coordinador.c -o generadores_coordinador -pthread -lrt
 Uso: ./generadores_coordinador <num_generadores> <total_registros> <salida.csv>
 Ej:  ./generadores_coordinador 4 100 salida.csv
*/

#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>           // O_* constants
#include <sys/mman.h>        // shm_open, mmap
#include <sys/stat.h>        // mode constants
#include <semaphore.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>

#define SHM_NAME "/shm_generadores_coord_v1"
// nombres base para semáforos
#define SEM_ITEMS_NAME "/sem_items_v1" // cuenta registros llenos listos para coord
#define SEM_ID_MUTEX "/sem_id_mutex_v1" // protege shared->next_id y remaining

// Generador i tendrá semáforos nombrados derivados:
void sem_name_slot_empty(char *buf, int idx){ sprintf(buf, "/sem_slot_empty_%d_v1", idx); }
void sem_name_slot_mutex(char *buf, int idx){ sprintf(buf, "/sem_slot_mutex_%d_v1", idx); }
void sem_name_id_req(char *buf, int idx){ sprintf(buf, "/sem_id_req_%d_v1", idx); }
void sem_name_id_grant(char *buf, int idx){ sprintf(buf, "/sem_id_grant_%d_v1", idx); }

/* --- Estructuras compartidas en memoria --- */

#define MAX_FIELDS 3            // cantidad de campos extra por registro (además del ID)
#define ID_BATCH 10             // cada generador pide 10 IDs

typedef struct {
    int id;                     // ID asignado
    char field[MAX_FIELDS][32]; // campos de ejemplo (nombres, valores, etc)
} record_t;

typedef struct {
    // por cada generador hay un "slot" para enviar un registro al coordinador
    // el coordinador y el generador i usan slot i.
    int slot_flag;              // 0 = vacío, 1 = lleno (escrito por generador)
    record_t slot_record;       // el registro escrito
    // buffer para IDs asignados por el coordinador al generador i:
    int granted_count;          // cuántos IDs fueron otorgados (0..ID_BATCH)
    int granted_ids[ID_BATCH];  // IDs otorgados
} slot_t;

typedef struct {
    int num_generators;
    int total_records;          // meta total que debe escribirse
    int written_records;        // cuántos registros ya escribió el coordinador
    int next_id;                // siguiente ID disponible (lo administra el coordinador)
    int remaining;              // cuantos IDs/regs faltan distribuir (inicia total_records)
    // seguida de array de slots[num_generators]
    // slot_t slots[];
} shm_header_t;

/* --- Variables globales usadas por el coordinador (en el proceso padre) --- */

shm_header_t *shm_hdr = NULL;
slot_t *slots = NULL;
size_t shm_size = 0;
int shm_fd = -1;
sem_t *sem_items = NULL;     // cuenta total de slots llenos listos para procesar
sem_t *sem_id_mutex = NULL;  // protege next_id y remaining

int N; // num_generators

// helpers para abrir semáforos por nombre
sem_t* create_named_semaphore(const char *name, unsigned int value) {
    sem_unlink(name); // intentar limpiar previo (silencioso)
    sem_t *s = sem_open(name, O_CREAT | O_EXCL, 0600, value);
    if (s == SEM_FAILED) {
        perror("sem_open create failed");
        return NULL;
    }
    return s;
}
sem_t* open_named_semaphore(const char *name) {
    sem_t *s = sem_open(name, 0);
    if (s == SEM_FAILED) {
        perror("sem_open open failed");
        return NULL;
    }
    return s;
}

/* ------------------ Función que atiende las solicitudes de IDs (thread por generador) ------------------
   Esto permite que el proceso coordinador responda a las peticiones de bloques de 10 IDs
   realizadas por cada generador (hijo). El hilo espera en el semáforo de petición `id_req`
   y cuando se activa, asigna hasta 10 IDs (o menos si quedan menos) en la zona de SHM
   correspondiente al generador, luego despierta al generador con `id_grant`.
*/
typedef struct {
    int idx;
} thread_arg_t;

void *id_request_handler(void *arg) {
    thread_arg_t *targ = (thread_arg_t *)arg;
    int idx = targ->idx;
    char name_req[64], name_grant[64];
    sem_name_id_req(name_req, idx);
    sem_name_id_grant(name_grant, idx);

    sem_t *sem_req = open_named_semaphore(name_req);
    sem_t *sem_grant = open_named_semaphore(name_grant);
    if (!sem_req || !sem_grant) {
        fprintf(stderr, "Fallo abrir semáforos id_req/id_grant para %d\n", idx);
        return NULL;
    }

    while (1) {
        // espera que el generador pida IDs
        sem_wait(sem_req);

        // proteger acceso a next_id / remaining
        sem_wait(sem_id_mutex);
        if (shm_hdr->remaining <= 0) {
            // No quedan IDs: otorgamos 0 para que el generador termine
            slots[idx].granted_count = 0;
            sem_post(sem_id_mutex);
            sem_post(sem_grant); // desbloquea al generador para que vea granted_count==0 y salga
            break;
        }
        int give = (shm_hdr->remaining >= ID_BATCH) ? ID_BATCH : shm_hdr->remaining;
        for (int i = 0; i < give; ++i) {
            slots[idx].granted_ids[i] = shm_hdr->next_id++;
        }
        slots[idx].granted_count = give;
        shm_hdr->remaining -= give;
        sem_post(sem_id_mutex);

        // avisar al generador que puede leer sus IDs
        sem_post(sem_grant);

        // si after assigning we have no remaining, continue loop: next requests will get 0 and exit
    }
    return NULL;
}

/* ------------------ Coordinador: procesa registros que van llegando en los slots ------------------
   Esto permite que el coordinador guarde en el archivo CSV cada registro enviado por los generadores.
   Usa sem_items para saber cuántos registros están listos (contador multi-proveedor).
*/
void coordinador_process_loop(FILE *csv_fp) {
    // abrir semáforos slot_mutex para cada slot (necesario para sincronizar lectura de slot_flag)
    sem_t **slot_mutex = calloc(N, sizeof(sem_t*));
    if (!slot_mutex) { perror("calloc"); exit(1); }
    for (int i = 0; i < N; ++i) {
        char nm[64];
        sem_name_slot_mutex(nm, i);
        slot_mutex[i] = open_named_semaphore(nm);
        if (!slot_mutex[i]) { fprintf(stderr, "Error abrir slot_mutex %d\n", i); exit(1); }
    }

    while (1) {
        // esperamos hasta que haya al menos un registro lleno
        sem_wait(sem_items);

        // buscar un slot con flag==1
        int found = -1;
        for (int i = 0; i < N; ++i) {
            // proteger lectura de slot_flag con slot_mutex
            sem_wait(slot_mutex[i]);
            if (slots[i].slot_flag == 1) {
                found = i;
                // leeremos y vaciaremos el slot aquí
                record_t rec = slots[i].slot_record;
                slots[i].slot_flag = 0;
                sem_post(slot_mutex[i]); // liberar mutex del slot
                // ahora podemos postar slot_empty para que el generador pueda volver a usar su slot
                char name_empty[64];
                sem_name_slot_empty(name_empty, i);
                sem_t *sem_empty = open_named_semaphore(name_empty);
                if (!sem_empty) { fprintf(stderr, "Error abrir slot_empty %d\n", i); exit(1); }
                sem_post(sem_empty);
                sem_close(sem_empty); // cerrar descriptor local
                // escribir en CSV
                fprintf(csv_fp, "%d", rec.id);
                for (int f = 0; f < MAX_FIELDS; ++f) {
                    fprintf(csv_fp, ",%s", rec.field[f]);
                }
                fprintf(csv_fp, "\n");
                fflush(csv_fp);

                shm_hdr->written_records++;
                // si ya escribimos la meta, terminamos (pero antes coordinador puede dejar que hilos den 0 IDs y niños terminen)
                if (shm_hdr->written_records >= shm_hdr->total_records) {
                    // limpiamos y nos vamos
                    for (int j=0;j<N;++j) sem_close(slot_mutex[j]);
                    free(slot_mutex);
                    return;
                }
                break; // procesamos un registro por iteración del sem_items pull
            } else {
                sem_post(slot_mutex[i]); // liberar si no está lleno
            }
        }
        // Nota: en condiciones normales siempre se encontrará un slot porque sem_items indica precisamente eso.
    }
}

/* ------------------ Código del generador (proceso hijo) ------------------
   Flujo:
   - pide IDs al coordinador (post a id_req y espera id_grant)
   - recibe hasta 10 IDs y para cada ID genera un registro aleatorio
   - por cada registro: espera slot_empty, toma slot_mutex, escribe slot_record, marca slot_flag=1, post items
   - cuando granted_count==0 el generador termina
*/
void generador_main(int idx) {
    // abrir semáforos que usa el generador
    char name_req[64], name_grant[64], name_empty[64], name_mutex[64];
    sem_name_id_req(name_req, idx);
    sem_name_id_grant(name_grant, idx);
    sem_name_slot_empty(name_empty, idx);
    sem_name_slot_mutex(name_mutex, idx);

    sem_t *sem_req = open_named_semaphore(name_req);
    sem_t *sem_grant = open_named_semaphore(name_grant);
    sem_t *sem_empty = open_named_semaphore(name_empty);
    sem_t *sem_mutex = open_named_semaphore(name_mutex);
    if (!sem_req || !sem_grant || !sem_empty || !sem_mutex) {
        fprintf(stderr, "Generador %d: error abrir sems\n", idx);
        exit(1);
    }

    srand(time(NULL) ^ (getpid()<<16));

    while (1) {
        // Esto permite que el generador solicite al coordinador el próximo bloque de hasta 10 IDs
        sem_post(sem_req);    // pedir IDs
        sem_wait(sem_grant);  // esperar a que el coordinador otorgue IDs (o 0 para terminar)

        int cnt = slots[idx].granted_count;
        if (cnt <= 0) break; // nada más por generar

        for (int k = 0; k < cnt; ++k) {
            int myid = slots[idx].granted_ids[k];

            // generar datos aleatorios para el registro
            record_t rec;
            rec.id = myid;
            // Ejemplos simples: nombre de fruta, color, número aleatorio
            const char *frutas[] = {"Manzana","Pera","Naranja","Banana","Kiwi"};
            const char *colores[] = {"Rojo","Verde","Amarillo","Azul","Negro"};
            snprintf(rec.field[0], sizeof(rec.field[0]), "%s", frutas[rand() % (sizeof(frutas)/sizeof(frutas[0]))]);
            snprintf(rec.field[1], sizeof(rec.field[1]), "%s", colores[rand() % (sizeof(colores)/sizeof(colores[0]))]);
            snprintf(rec.field[2], sizeof(rec.field[2]), "%d", rand() % 1000);

            // enviar registro al coordinador mediante SHM y semáforos del slot
            sem_wait(sem_empty);   // esperar que el slot esté vacío
            sem_wait(sem_mutex);   // entrar sección crítica del slot
            slots[idx].slot_record = rec;
            slots[idx].slot_flag = 1;
            sem_post(sem_mutex);
            // avisar al coordinador que hay un item listo
            sem_post(sem_items);
            // el coordinador liberará slot_empty cuando procese el slot
        }
    }

    // cerrar semáforos abiertos
    sem_close(sem_req); sem_close(sem_grant); sem_close(sem_empty); sem_close(sem_mutex);
    // finalizar proceso hijo
    exit(0);
}

/* ------------------ Inicialización de SHM y semáforos ------------------ */
void cleanup_ipc(int n) {
    // unlink semáforos nombrados y shm
    if (sem_items) { sem_close(sem_items); sem_unlink(SEM_ITEMS_NAME); sem_items = NULL; }
    if (sem_id_mutex) { sem_close(sem_id_mutex); sem_unlink(SEM_ID_MUTEX); sem_id_mutex = NULL; }

    for (int i = 0; i < n; ++i) {
        char nm[64];
        sem_name_slot_empty(nm, i); sem_unlink(nm);
        sem_name_slot_mutex(nm, i); sem_unlink(nm);
        sem_name_id_req(nm, i); sem_unlink(nm);
        sem_name_id_grant(nm, i); sem_unlink(nm);
    }
    if (shm_fd != -1) {
        munmap(shm_hdr, shm_size);
        close(shm_fd);
        shm_unlink(SHM_NAME);
        shm_fd = -1;
    }
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Uso: %s <num_generadores> <total_registros> <salida.csv>\n", argv[0]);
        exit(1);
    }
    N = atoi(argv[1]);
    int total = atoi(argv[2]);
    char *csv_file = argv[3];
    if (N <= 0 || total <= 0) {
        fprintf(stderr, "Parametros invalidos\n");
        exit(1);
    }

    // calcular tamaño de SHM: header + N slots
    shm_size = sizeof(shm_header_t) + N * sizeof(slot_t);
    shm_fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (shm_fd == -1) {
        perror("shm_open");
        exit(1);
    }
    if (ftruncate(shm_fd, shm_size) == -1) { perror("ftruncate"); shm_unlink(SHM_NAME); exit(1); }
    void *shm_ptr = mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED) { perror("mmap"); shm_unlink(SHM_NAME); exit(1); }

    shm_hdr = (shm_header_t *)shm_ptr;
    slots = (slot_t *)((char*)shm_ptr + sizeof(shm_header_t));

    // inicializar la estructura compartida
    shm_hdr->num_generators = N;
    shm_hdr->total_records = total;
    shm_hdr->written_records = 0;
    shm_hdr->next_id = 1; // IDs comienzan en 1
    shm_hdr->remaining = total;
    for (int i = 0; i < N; ++i) {
        slots[i].slot_flag = 0;
        slots[i].granted_count = 0;
        // memset slots por seguridad
        memset(&slots[i].slot_record, 0, sizeof(record_t));
    }

    // crear semáforos nombrados
    sem_unlink(SEM_ITEMS_NAME);
    sem_unlink(SEM_ID_MUTEX);
    sem_items = sem_open(SEM_ITEMS_NAME, O_CREAT | O_EXCL, 0600, 0); // inicialmente 0 items
    sem_id_mutex = sem_open(SEM_ID_MUTEX, O_CREAT | O_EXCL, 0600, 1);
    if (sem_items == SEM_FAILED || sem_id_mutex == SEM_FAILED) {
        perror("sem_open global");
        cleanup_ipc(N);
        exit(1);
    }

    // crear semáforos por slot / id
    for (int i = 0; i < N; ++i) {
        char name_empty[64], name_mutex[64], name_req[64], name_grant[64];
        sem_name_slot_empty(name_empty, i); sem_unlink(name_empty);
        sem_name_slot_mutex(name_mutex, i); sem_unlink(name_mutex);
        sem_name_id_req(name_req, i); sem_unlink(name_req);
        sem_name_id_grant(name_grant, i); sem_unlink(name_grant);

        // slot_empty inicial 1 (slot libre), slot_mutex 1 (libre)
        sem_t *s_empty = sem_open(name_empty, O_CREAT | O_EXCL, 0600, 1);
        sem_t *s_mutex = sem_open(name_mutex, O_CREAT | O_EXCL, 0600, 1);
        sem_t *s_req = sem_open(name_req, O_CREAT | O_EXCL, 0600, 0);
        sem_t *s_grant = sem_open(name_grant, O_CREAT | O_EXCL, 0600, 0);
        if (s_empty == SEM_FAILED || s_mutex == SEM_FAILED || s_req == SEM_FAILED || s_grant == SEM_FAILED) {
            perror("sem_open per-slot");
            cleanup_ipc(N);
            exit(1);
        }
        sem_close(s_empty); sem_close(s_mutex); sem_close(s_req); sem_close(s_grant);
    }

    // abrir archivo CSV e imprimir cabecera
    FILE *csv_fp = fopen(csv_file, "w");
    if (!csv_fp) { perror("fopen csv"); cleanup_ipc(N); exit(1); }
    // Esto permite que el archivo CSV tenga la primera fila con los nombres de columna
    // El ID es la primera columna, luego campos de ejemplo
    fprintf(csv_fp, "ID,Fruta,Color,Numero\n");
    fflush(csv_fp);

    // crear hilos en el coordinador para atender solicitudes de IDs
    pthread_t *threads = calloc(N, sizeof(pthread_t));
    thread_arg_t *targs = calloc(N, sizeof(thread_arg_t));
    for (int i = 0; i < N; ++i) {
        targs[i].idx = i;
        if (pthread_create(&threads[i], NULL, id_request_handler, &targs[i]) != 0) {
            perror("pthread_create");
            cleanup_ipc(N);
            exit(1);
        }
    }

    // forkar N generadores (procesos hijos)
    for (int i = 0; i < N; ++i) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); cleanup_ipc(N); exit(1); }
        if (pid == 0) {
            // proceso hijo: generador
            // mapear la misma SHM (ya está mapeada en memoria padre, pero en hijo necesitamos mapearla también)
            int fd = shm_open(SHM_NAME, O_RDWR, 0);
            if (fd == -1) { perror("child shm_open"); exit(1); }
            void *p = mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (p == MAP_FAILED) { perror("child mmap"); exit(1); }
            // reasignar punteros locales a la SHM
            shm_hdr = (shm_header_t *)p;
            slots = (slot_t *)((char*)p + sizeof(shm_header_t));
            // abrir sem_items también
            sem_items = open_named_semaphore(SEM_ITEMS_NAME);
            sem_id_mutex = open_named_semaphore(SEM_ID_MUTEX);
            // ejecutar código del generador index i
            generador_main(i);
            // nunca llega aquí
        }
    }

    // proceso padre: coordinador
    // el padre mapea ya el SHM (shm_hdr, slots están listos)
    // abrir sem_items y sem_id_mutex (ya creados)
    // reusar sem_items y sem_id_mutex creados arriba

    // Esto permite que el coordinador guarde cada registro recibido en el archivo CSV
    coordinador_process_loop(csv_fp);

    // Esperar a que hijos terminen
    for (int i = 0; i < N; ++i) wait(NULL);

    // avisar a hilos que terminen: cada hilo saldrá cuando vea que remaining<=0 y un request venga con granted_count=0
    // Como los niños ya terminaron y no van a pedir más, posteamos a cada sem_id_req para que cada hilo pueda salir
    for (int i = 0; i < N; ++i) {
        char name_req[64];
        sem_name_id_req(name_req, i);
        sem_t *sr = open_named_semaphore(name_req);
        if (sr) { sem_post(sr); sem_close(sr); }
    }

    // join threads
    for (int i = 0; i < N; ++i) pthread_join(threads[i], NULL);

    // limpieza
    fclose(csv_fp);
    cleanup_ipc(N);

    free(threads);
    free(targs);

    printf("Coordinador: terminado. Registros escritos: %d\n", total);
    return 0;
}


