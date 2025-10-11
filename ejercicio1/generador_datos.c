#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <time.h>
#include <string.h>
#include <signal.h>
#include <errno.h>

// --- Constantes
#define CLAVE_SHM 1234
#define CLAVE_SEM 5678
#define TAMANIO_BLOQUE_IDS 10 // Bloque de IDs que cada generador solicita
#define LONGITUD_MAXIMA_DATOS 50
#define NOMBRE_ARCHIVO_CSV "registros_generados.csv"

// --- Estructuras para IPC
// Estructura que se compartirá en la memoria compartida (SHM)
typedef struct {
    int proximo_id_a_asignar; // Usado por el Coordinador para asignar IDs
    int total_registros_generados; // Contador de registros escritos
    int total_objetivo_registros; // Total de registros a generar (parámetro de entrada)
    int hay_datos_disponibles; // Flag: 0 = No hay datos nuevos, 1 = Hay datos para el coordinador
    int finalizado; // Flag: 1 = Todos los generadores han terminado
    int generadores_finalizados; // Contador de generadores que finalizaron

    // El 'registro', para ser enviado del Generador al Coordinador
    struct {
        int id;
        char nombre_producto[LONGITUD_MAXIMA_DATOS];
        int cantidad;
        float precio;
    } buffer_registro;
} DatosCompartidos;

// Definición para el uso de semctl (necesario en Linux)
#if defined(__GNUC__) && !defined(_GNU_SOURCE)
union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
    struct seminfo *__buf;
};
#endif

// --- Manejo controlado de finalización (Requisito 8)
static volatile sig_atomic_t detener_solicitado = 0; // Señal de parada por SIGINT/SIGTERM
static volatile sig_atomic_t generadores_en_ejecucion = 0; // Cantidad de hijos vivos
static int g_id_sem = -1;
static int g_id_shm = -1;
static DatosCompartidos *g_datos_compartidos = NULL;
//Si el usuario presiona Ctrl+C, se detiene el programa
static void manejador_sigint(int sig) {
    (void)sig;
    detener_solicitado = 1;
    printf("\n[Señal] SIGINT recibida. Finalizando programa...\n");
}
//Si el usuario jace un kill-9 al programa, se detiene el programa
static void manejador_sigterm(int sig) {
    (void)sig;
    detener_solicitado = 1;
}
//Si el hijo finaliza, se actualiza la cantidad de hijos vivos
static void manejador_sigchld(int sig) {
    (void)sig;
    int status;
    // Recolectar todos los hijos finalizados sin bloquear
    while (waitpid(-1, &status, WNOHANG) > 0) {
        if (generadores_en_ejecucion > 0) {
            generadores_en_ejecucion--;
        }
    }
}

// --- Prototipos de funciones
void proceso_coordinador(int id_shm, int id_sem, int cantidad_generadores, int total_registros);
void proceso_generador(int id_shm, int id_sem, int id_generador);
void sem_esperar(int id_sem, int indice_sem);
void sem_senalizar(int id_sem, int indice_sem);

// Indices para el conjunto de semáforos
#define SEM_ASIGNACION_ID 0  // Para proteger 'proximo_id_a_asignar' (Coordinador)
#define SEM_DATOS_PROD_CONS 1 // Para el patrón Productor-Consumidor (Generador/Coordinador)

// --- Función para la lógica del Generador
// ---------------------------------------------------------------------GENERADOR 
void proceso_generador(int id_shm, int id_sem, int id_generador) {
    DatosCompartidos *shm_data = (DatosCompartidos *)shmat(id_shm, NULL, 0);
    if (shm_data == (void *)-1) {
        perror("Error al adjuntar SHM en Generador");
        exit(1);
    }

    // Instalar manejador de señales para el generador
    struct sigaction sa_int;
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = manejador_sigint;
    sigemptyset(&sa_int.sa_mask);
    sa_int.sa_flags = 0;
    sigaction(SIGINT, &sa_int, NULL);

    struct sigaction sa_term;
    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = manejador_sigterm;
    sigemptyset(&sa_term.sa_mask);
    sa_term.sa_flags = 0;
    sigaction(SIGTERM, &sa_term, NULL);

    srand(time(NULL) + getpid()); // Inicialización de semilla aleatoria por proceso

    int my_start_id = -1;
    int my_end_id = -1;
    int current_id = -1;

    printf("[Generador %d] Proceso iniciado.\n", id_generador);

    // Lista de productos aleatorios para la simulación
    const char *productos[] = {"Laptop", "Smartphone", "Tablet", "Monitor", "Teclado", "Mouse", "Impresora"};
    int num_productos = sizeof(productos) / sizeof(productos[0]);

    // Bucle principal: generar registros mientras haya IDs disponibles
    while (1) {
        // Salida temprana si el Coordinador indicó finalizar o se recibió señal
        if (shm_data->finalizado || detener_solicitado) {
            break;
        }
        // 1. Solicitud y obtención de bloque de IDs
        if (current_id > my_end_id || current_id == -1) {
            // Esto permite al proceso Generador solicitar un nuevo bloque de 10 IDs al Coordinador.
            sem_esperar(id_sem, SEM_ASIGNACION_ID);

            int next_available_id = shm_data->proximo_id_a_asignar;

            // Verificar si quedan IDs para asignar
            if (next_available_id > shm_data->total_objetivo_registros) {
                sem_senalizar(id_sem, SEM_ASIGNACION_ID);
                break; // No quedan más registros por generar
            }

            my_start_id = next_available_id;
            my_end_id = next_available_id + TAMANIO_BLOQUE_IDS - 1;

            // Ajustar el final del bloque si se excede el total
            if (my_end_id > shm_data->total_objetivo_registros) {
                my_end_id = shm_data->total_objetivo_registros;
            }

            shm_data->proximo_id_a_asignar = my_end_id + 1;
            current_id = my_start_id;

            printf("[Generador %d] Recibi IDs: %d a %d.\n", id_generador, my_start_id, my_end_id);

            sem_senalizar(id_sem, SEM_ASIGNACION_ID);
        }

        // 2. Generación y envío de registros uno por uno
        if (current_id <= my_end_id) {
            // Esto permite al proceso Generador esperar a que la memoria compartida est� libre
            // (el Coordinador haya procesado el registro anterior).
            sem_esperar(id_sem, SEM_DATOS_PROD_CONS);

            if (shm_data->finalizado || detener_solicitado) {
                // Si el Coordinador decidió finalizar o se recibió señal, liberar y salir
                sem_senalizar(id_sem, SEM_DATOS_PROD_CONS);
                break;
            }

            if (shm_data->hay_datos_disponibles) {
                // Esto es una verificación de seguridad, en un esquema de productor-consumidor simple���//��
                // el Generador no debería escribir si ya hay datos pendientes.
                sem_senalizar(id_sem, SEM_DATOS_PROD_CONS);
                continue;
            }

            // Generar datos aleatorios
            shm_data->buffer_registro.id = current_id;
            strcpy(shm_data->buffer_registro.nombre_producto, productos[rand() % num_productos]);
            shm_data->buffer_registro.cantidad = rand() % 100 + 1; // 1 a 100
            shm_data->buffer_registro.precio = (float)(rand() % 5000 + 100) / 100.0; // Precio entre 1.00 y 50.99

            // Senialar al Coordinador que hay un registro listo
            // Esto permite al proceso Generador notificar al Coordinador que un nuevo registro está disponible en la memoria compartida.
             
            shm_data->hay_datos_disponibles = 1;

            printf("[Generador %d] Produjo ID %d.\n", id_generador, current_id);

            current_id++;

            sem_senalizar(id_sem, SEM_DATOS_PROD_CONS);

            // Pequeniaa espera para no monopolizar la CPU
            usleep(rand() % 100000); // 0 a 100ms
        } else {
            // Ya se terminaron los IDs del bloque, el bucle lo detectará al inicio para pedir uno nuevo.
        }
    }

    if (detener_solicitado) {
        printf("[Generador %d] Finalizado por señal. Detaching SHM.\n", id_generador);
    } else {
        printf("[Generador %d] Finalizado. Detaching SHM.\n", id_generador);
    }
    // Informar al coordinador que este generador finaliza
    sem_esperar(id_sem, SEM_ASIGNACION_ID);
    shm_data->generadores_finalizados++;
    sem_senalizar(id_sem, SEM_ASIGNACION_ID);
    shmdt(shm_data);
    exit(0);
}

// --- Funcion para la lógica del Coordinador
void proceso_coordinador(int id_shm, int id_sem, int cantidad_generadores, int total_registros) {
    DatosCompartidos *shm_data = (DatosCompartidos *)shmat(id_shm, NULL, 0);
    if (shm_data == (void *)-1) {
        perror("Error al adjuntar SHM en Coordinador");
        exit(1);
    }

    // Registrar punteros/ids globales para manejadores y estado
    g_datos_compartidos = shm_data;
    g_id_sem = id_sem;
    g_id_shm = id_shm;
    generadores_en_ejecucion = cantidad_generadores;

    // Instalar manejadores de señales
    struct sigaction sa_int;
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = manejador_sigint;
    sigemptyset(&sa_int.sa_mask);
    sa_int.sa_flags = 0;
    sigaction(SIGINT, &sa_int, NULL);

    struct sigaction sa_term;
    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = manejador_sigterm;
    sigemptyset(&sa_term.sa_mask);
    sa_term.sa_flags = 0;
    sigaction(SIGTERM, &sa_term, NULL);

    struct sigaction sa_chld;
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = manejador_sigchld;
    sigemptyset(&sa_chld.sa_mask);
    sa_chld.sa_flags = SA_NOCLDSTOP | SA_RESTART;
    sigaction(SIGCHLD, &sa_chld, NULL);

    FILE *csv_file = fopen(NOMBRE_ARCHIVO_CSV, "w");
    if (!csv_file) {
        perror("Error al abrir el archivo CSV");
        shmdt(shm_data);
        exit(1);
    }

    // Esto permite al proceso Coordinador escribir los nombres de las columnas
    // en la primera l�nea del archivo CSV, cumpliendo con el requisito.
    fprintf(csv_file, "ID;Producto;Cantidad;Precio\n");
    printf("[Coordinador] Archivo CSV inicializado con encabezado.\n");

    // Bucle principal del Coordinador: recibir y escribir registros
    while ((shm_data->total_registros_generados < total_registros) && (generadores_en_ejecucion > 0) && !detener_solicitado) {
        // Esto permite al proceso Coordinador esperar nuevos datos en la memoria compartida.
        sem_esperar(id_sem, SEM_DATOS_PROD_CONS);

        if (shm_data->hay_datos_disponibles) {
            // Escribir el registro recibido en el archivo CSV
            fprintf(csv_file, "%d;%s;%d;%.2f\n",
                    shm_data->buffer_registro.id,
                    shm_data->buffer_registro.nombre_producto,
                    shm_data->buffer_registro.cantidad,
                    shm_data->buffer_registro.precio);

            fflush(csv_file); // Asegurar que se escriba inmediatamente en el archivo

            shm_data->total_registros_generados++;
            printf("[Coordinador] Escribi� registro ID %d. Total: %d/%d\n", shm_data->buffer_registro.id, shm_data->total_registros_generados, total_registros);

            // Marcar el b�fer como vac�o para que un generador pueda escribir el siguiente
            // Esto permite al proceso Coordinador liberar la memoria compartida para el pr�ximo
            // registro del Generador, manteniendo el flujo Productor-Consumidor.
            shm_data->hay_datos_disponibles = 0;

        } else {
            // Si no hay datos, y ya se gener� la cantidad objetivo, podemos terminar.
            if (shm_data->total_registros_generados >= total_registros) {
                 shm_data->finalizado = 1;
            }
        }

        sem_senalizar(id_sem, SEM_DATOS_PROD_CONS);

        // Comprobación de terminación o señal de parada
        if (shm_data->total_registros_generados >= total_registros || detener_solicitado || generadores_en_ejecucion == 0) {
            break;
        }

        // Pequea espera para no hacer busy-waiting intenso
        usleep(1000);
    }

    // Indicar a los generadores que deben finalizar
    shm_data->finalizado = 1;

    // Esperar a que todos los generadores confirmen salida
    while (shm_data->generadores_finalizados < cantidad_generadores) {
        usleep(1000);
    }

    fclose(csv_file);
    if (detener_solicitado) {
        printf("[Coordinador] Finalizado por señal. Total de registros generados: %d.\n", shm_data->total_registros_generados);
    } else {
        printf("[Coordinador] Finalizado. Total de registros generados: %d.\n", shm_data->total_registros_generados);
    }

    // Esperar a que todos los procesos generadores terminen
    int status;
    // while ((wait(&status)) > 0); // Este bucle ya está manejado por handle_sigchld

    printf("[Coordinador] Todos los Generadores terminaron. Limpiando IPC.\n");

    // Limpieza de IPC
    shmdt(shm_data);
    shmctl(id_shm, IPC_RMID, NULL); // Eliminar la memoria compartida
    semctl(id_sem, 0, IPC_RMID);    // Eliminar el conjunto de semforos
}

// --- MAIN
int main(int argc, char *argv[]) {
    if (argc != 3) {
        // Esto permite al programa especificar por par�metro la cantidad de procesos generadores y
        // la cantidad total de registros a generar, cumpliendo con el requisito.
        fprintf(stderr, "Uso: %s <num_generadores> <total_registros>\n", argv[0]);
        return 1;
    }

    int cantidad_generadores = atoi(argv[1]);
    int total_registros = atoi(argv[2]);

    if (cantidad_generadores <= 0 || total_registros <= 0) {
        fprintf(stderr, "Ambos valores deben ser positivos.\n");
        return 1;
    }

    // --- 1. Inicializaci�n de Memoria Compartida (SHM)
    int shmid = shmget(CLAVE_SHM, sizeof(DatosCompartidos), IPC_CREAT | 0666);
    if (shmid < 0) {
        perror("Error al crear SHM");
        return 1;
    }

    DatosCompartidos *shm_data = (DatosCompartidos *)shmat(shmid, NULL, 0);
    if (shm_data == (void *)-1) {
        perror("Error al adjuntar SHM");
        shmctl(shmid, IPC_RMID, NULL);
        return 1;
    }

    // Inicializaci�n de datos
    shm_data->proximo_id_a_asignar = 1;  // Empezar desde ID 1
    shm_data->total_registros_generados = 0;
    shm_data->total_objetivo_registros = total_registros;
    shm_data->hay_datos_disponibles = 0;
    shm_data->finalizado = 0;
    shm_data->generadores_finalizados = 0;

    // --- 2. Inicializacin de Semforos
    // Creamos 2 semforos: uno para la asignacin de IDs y otro para el bfer de datos (productor/consumidor)
    int semid = semget(CLAVE_SEM, 2, IPC_CREAT | 0666);
    if (semid < 0) {
        perror("Error al crear SEM");
        shmdt(shm_data);
        shmctl(shmid, IPC_RMID, NULL);
        return 1;
    }

    // Inicializar el sem�foro de asignaci�n de IDs (mutua exclusi�n binaria: 1)
    // Esto permite que solo un proceso a la vez acceda a las variables compartidas de conteo de IDs.
    union semun arg;
    arg.val = 1;
    semctl(semid, SEM_ASIGNACION_ID, SETVAL, arg);

    // Inicializar el sem�foro de datos (mutua exclusi�n binaria: 1)
    // Esto permite un control estricto sobre qui�n usa el b�fer de registro en SHM (Generador *o* Coordinador).
    arg.val = 1;
    semctl(semid, SEM_DATOS_PROD_CONS, SETVAL, arg);


    // --- 3. Creaci�n de Procesos Generadores (hijos)
    for (int i = 0; i < cantidad_generadores; i++) {
        pid_t pid = fork();

        if (pid < 0) {
            perror("Error al hacer fork");
            // Aqu� se deber�a limpiar m�s exhaustivamente, pero simplificamos
            break;
        } else if (pid == 0) {
            // Proceso Generador (Hijo)
            shmdt(shm_data); // El hijo se desadjunta del puntero inicial y lo adjunta en su funci�n
            proceso_generador(shmid, semid, i + 1);
            // El proceso hijo termina en generator_process(exit(0))
        }
    }

    // Proceso Coordinador (Padre)
    // Desadjuntarse temporalmente para luego adjuntarse correctamente en la funci�n coordinadora
    shmdt(shm_data);
    proceso_coordinador(shmid, semid, cantidad_generadores, total_registros);

    return 0;
}

// --- Funciones auxiliares de Semáforo
// Operación P (Wait): Disminuye el valor del semáforo. Si es 0, espera.
void sem_esperar(int id_sem, int indice_sem) {
    struct sembuf sb = {indice_sem, -1, 0};
    // Esto permite a un proceso Generador o Coordinador bloquear el acceso
    // a una secci�n cr�tica (como la asignaci�n de IDs o el b�fer de datos).
    if (semop(id_sem, &sb, 1) == -1) {
        perror("sem_wait fall�");
        exit(1);
    }
}

// Operación V (Signal): Aumenta el valor del semáforo, liberando a un proceso en espera.
void sem_senalizar(int id_sem, int indice_sem) {
    struct sembuf sb = {indice_sem, 1, 0};
    // Esto permite a un proceso Generador o Coordinador liberar el acceso
    // despu�s de usar una secci�n cr�tica, permitiendo que otro proceso contin�e.
    if (semop(id_sem, &sb, 1) == -1) {
        perror("sem_signal fall�");
        exit(1);
    }
}

