#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/file.h> // Para la función 'flock'
#include <signal.h>
#include <sys/wait.h>
#include <math.h>

// --- Constantes y Configuración
#define MAX_COMMAND_LENGTH 512
#define CSV_FILE_NAME "registros_generados.csv"
#define BACKLOG_QUEUE 5 // M clientes en espera (Requisito 1: M)
#define MAX_CLIENTS 5 // N clientes concurrentes (Requisito 1: N)
#define DEFAULT_PORT 8080

// --- Variables Globales de Estado
volatile int active_clients = 0;
pthread_mutex_t clients_mutex;
int is_file_locked = 0; // Estado: 1 si hay una transacción activa
int locked_by_socket = -1; // Socket del cliente que tiene el lock
pthread_mutex_t lock_status_mutex; // Mutex para proteger is_file_locked y locked_by_socket
int locked_fd = -1; // Descriptor del archivo bloqueado durante la transacción
static int server_fd_global = -1; // socket de escucha global para cierre seguro
static volatile int next_user_id = 1;
pthread_cond_t clients_cond; // Señaliza disponibilidad de slots de cliente

// --- Prototipos
void *handle_client(void *arg);
int try_acquire_lock(int client_socket);
void release_lock(int client_socket);
void load_config(char *ip, int *port, int *max_clients, int *backlog);
char *execute_query(const char *command, int *is_success);
char *perform_modification(const char *command, int *is_success);
static void cleanup_resources(void);
static void handle_termination_signal(int signum);

// Estructura para pasar argumentos al hilo
typedef struct {
    int socket;
    struct sockaddr_in address;
    int user_id;
} client_info;

// --- Funciones de Bloqueo (Transacciones)

/**
 * Esto permite al cliente iniciar una transacción y obtener un bloqueo (lock) exclusivo sobre el archivo CSV.
 */
int try_acquire_lock(int client_socket) {
    int result = 0;

    // Abrimos el archivo en modo lectura/escritura
    int fd = open(CSV_FILE_NAME, O_RDWR);
    if (fd < 0) return -1; // Error de archivo

    // Intentamos un bloqueo exclusivo (LOCK_EX) sin esperar (LOCK_NB)
    if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
        // Bloqueo exitoso
        pthread_mutex_lock(&lock_status_mutex);
        is_file_locked = 1;
        locked_by_socket = client_socket;
        locked_fd = fd; // mantener descriptor abierto para sostener el lock
        pthread_mutex_unlock(&lock_status_mutex);
        result = 1; // éxito
    } else {
        // Bloqueo fallido (ya bloqueado por otro)
        result = 0; // Fallo
        close(fd);
    }
    return result;
}

/**
 * Esto permite al cliente confirmar la transaccin (COMMIT) y liberar el bloqueo exclusivo.
 */
void release_lock(int client_socket) {
    pthread_mutex_lock(&lock_status_mutex);
    if (is_file_locked && locked_by_socket == client_socket) {
        // Liberar el bloqueo sobre el descriptor sostenido
        if (locked_fd >= 0) {
            flock(locked_fd, LOCK_UN);
            close(locked_fd);
            locked_fd = -1;
        }
        is_file_locked = 0;
        locked_by_socket = -1;
    }
    pthread_mutex_unlock(&lock_status_mutex);
}

// --- Manejo de Cliente Concurrente

/**
 * Esto permite al servidor aceptar hasta N clientes concurrentes, manejando la conexin
 * de forma independiente en un thread, cumpliendo con el Requisito 1.
 */
void *handle_client(void *arg) {
    client_info *info = (client_info *)arg;
    int client_socket = info->socket;
    char buffer[MAX_COMMAND_LENGTH] = {0};
    int transaction_active = 0;

    printf("[THREAD %lu] Cliente conectado desde %s:%d\n",
           pthread_self(), inet_ntoa(info->address.sin_addr), ntohs(info->address.sin_port));

    // Enviar bienvenida con el identificador de usuario asignado
    char welcome[128];
    snprintf(welcome, sizeof(welcome), "Bienvenido. Usted es el Usuario %d. Use HELP para ayuda.\n", info->user_id);
    send(client_socket, welcome, strlen(welcome), 0);

    // Esto permite al cliente ser interactivo, manteniéndose conectado hasta que decida salir.
    while (1) {
        memset(buffer, 0, MAX_COMMAND_LENGTH);
        int valread = read(client_socket, buffer, MAX_COMMAND_LENGTH);

        if (valread <= 0) {
            // Cierre inesperado o desconexión normal
            printf("[THREAD %lu] Cliente desconectado (socket %d).\n", pthread_self(), client_socket);
            break;
        }

        char *command = buffer;
        command[strcspn(command, "\n")] = 0; // Eliminar newline

        if (strncmp(command, "EXIT", 4) == 0) {
            // Desconexión normal
            printf("[THREAD %lu] Comando EXIT recibido (socket %d).\n", pthread_self(), client_socket);
            break;
        }

        // --- 1. Manejo de Transacciones ---
        else if (strncmp(command, "BEGIN TRANSACTION", 17) == 0) {
            if (try_acquire_lock(client_socket)) {
                transaction_active = 1;
                send(client_socket, "OK: Transaccion iniciada. Lock exclusivo obtenido.\n", 50, 0);
            } else {
                // Si otro cliente lo intenta, debe recibir un error (Requisito 5)
                send(client_socket, "ERROR: Transaccion activa. Reintente luego.\n", 44, 0);
            }
        }
        else if (strncmp(command, "COMMIT TRANSACTION", 18) == 0) {
            if (transaction_active) {
                // Aquí iría la lógica de aplicar modificaciones si usáramos un log,
                // pero como modificamos directamente el archivo, solo liberamos el lock.
                release_lock(client_socket);
                transaction_active = 0;
                send(client_socket, "OK: Transaccion confirmada. Lock liberado.\n", 43, 0);
            } else {
                send(client_socket, "ERROR: No hay transaccion activa para hacer COMMIT.\n", 52, 0);
            }
        }

        // --- 2. Modificaciones (DML) ---
        else if (strncmp(command, "INSERT", 6) == 0 || strncmp(command, "UPDATE", 6) == 0 || strncmp(command, "DELETE", 6) == 0) {
            // Si hay otra transacción activa, rechazar
            pthread_mutex_lock(&lock_status_mutex);
            int locked = is_file_locked;
            int locked_by_me = (locked_by_socket == client_socket);
            pthread_mutex_unlock(&lock_status_mutex);

            if (locked && !locked_by_me) {
                send(client_socket, "ERROR: Transaccion activa en curso. Reintente luego.\n", 53, 0);
            } else if (!transaction_active) {
                send(client_socket, "ERROR: Las modificaciones requieren BEGIN TRANSACTION.\n", 55, 0);
            } else {
                // Esto permite realizar modificaciones de datos (alta, baja, modificación)
                // de forma segura dentro de una transacción.
                int success;
                char *response = perform_modification(command, &success);
                send(client_socket, response, strlen(response), 0);
                free(response);
            }
        }

        // --- 3. Consultas (SELECT) ---
        else if (strncmp(command, "SELECT", 6) == 0) {
            // Las consultas no necesitan un lock exclusivo, pero s deben esperar
            // a que NO haya una transaccin activa para asegurar datos consistentes.

            pthread_mutex_lock(&lock_status_mutex);
            int locked = is_file_locked;
            pthread_mutex_unlock(&lock_status_mutex);

            if (locked) {
                 // Si hay una transacción activa, ningún otro cliente puede realizar consultas (Requisito 5)
                send(client_socket, "ERROR: Transaccion activa en curso. Reintente luego.\n", 53, 0);
            } else {
                // Esto permite al cliente realizar consultas (ej. búsquedas, filtros, etc.).
                int success;
                char *response = execute_query(command, &success);
                send(client_socket, response, strlen(response), 0);
                free(response);
            }
        }
        else {
            send(client_socket, "ERROR: Comando no reconocido.\n", 30, 0);
        }
    }

    // --- Cleanup
    if (transaction_active) {
        // Manejo de cierre inesperado: si la transacción está activa, debe liberar el lock
        release_lock(client_socket);
        printf("[THREAD %lu] Advertencia: Cliente caido con transaccion activa. Lock liberado.\n", pthread_self());
    }

    close(client_socket);

    // Esto permite al servidor manejar el conteo de clientes concurrentes (Requisito 1)
    pthread_mutex_lock(&clients_mutex);
    active_clients--;
    printf("[Servidor] Clientes activos: %d.\n", active_clients);
    pthread_cond_signal(&clients_cond);
    pthread_mutex_unlock(&clients_mutex);

    free(info);
    pthread_exit(NULL);
}

// --- MAIN
int main(int argc, char *argv[]) {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);
    char ip[16] = "127.0.0.1";
    int port = DEFAULT_PORT;
    int max_clients_config = MAX_CLIENTS;
    int backlog_config = BACKLOG_QUEUE;

    // Esto permite al servidor obtener la dirección IP, nombre de host y puerto de escucha
    // por archivo de configuración o parámetro (en este caso, simulación simple por código).
    load_config(ip, &port, &max_clients_config, &backlog_config);

    // Argumentos opcionales para configurar N (concurrentes) y M (backlog)
    // Formatos:
    //   servidor                     -> valores por defecto/cargados
    //   servidor N M                 -> fija N y M
    //   servidor IP PUERTO N M       -> fija IP, PUERTO, N y M
    if (argc == 3) {
        max_clients_config = atoi(argv[1]);
        backlog_config = atoi(argv[2]);
    } else if (argc == 5) {
        strncpy(ip, argv[1], sizeof(ip) - 1);
        ip[sizeof(ip) - 1] = '\0';
        port = atoi(argv[2]);
        max_clients_config = atoi(argv[3]);
        backlog_config = atoi(argv[4]);
    } else if (argc != 1) {
        fprintf(stderr, "Uso: %s [N M] | %s [IP PUERTO N M]\n", argv[0], argv[0]);
        exit(EXIT_FAILURE);
    }

    if (max_clients_config <= 0 || backlog_config < 0 || port <= 0 || port > 65535) {
        fprintf(stderr, "Parametros invalidos. N>0, M>=0, 0<PUERTO<=65535.\n");
        exit(EXIT_FAILURE);
    }

    // Inicialización de mutexes
    pthread_mutex_init(&clients_mutex, NULL);
    pthread_mutex_init(&lock_status_mutex, NULL);
    pthread_cond_init(&clients_cond, NULL);

    // Manejo de señales: evitar caídas por SIGPIPE y limpieza en SIGINT/SIGTERM
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_termination_signal);
    signal(SIGTERM, handle_termination_signal);

    // Registrar limpieza en salida normal
    atexit(cleanup_resources);

    // Creación del socket del servidor
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == 0) { perror("socket failed"); exit(EXIT_FAILURE); }
    server_fd_global = server_fd;

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(ip);
    address.sin_port = htons(port);

    // Esto permite al servidor enlazar el socket a la dirección y puerto especificados.
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Esto permite al servidor escuchar nuevas conexiones y mantener hasta M clientes en espera (Requisito 1)
    if (listen(server_fd, backlog_config) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Servidor Micro DB escuchando en %s:%d. Max concurrentes (N): %d, Backlog (M): %d.\n", ip, port, max_clients_config, backlog_config);

    // Bucle principal: permanecer a la espera de nuevos clientes
    // Esto permite al servidor permanecer a la espera de nuevos clientes luego que todos los clientes
    // se desconectaron, sin terminar por sus propios medios (Requisito 6).
    while (1) {
        // Esperar hasta que haya espacio para otro cliente concurrente (N)
        pthread_mutex_lock(&clients_mutex);
        while (active_clients >= max_clients_config) {
            pthread_cond_wait(&clients_cond, &clients_mutex);
        }
        pthread_mutex_unlock(&clients_mutex);

        client_info *info = (client_info *)malloc(sizeof(client_info));
        if (!info) { perror("malloc failed"); continue; }

        new_socket = accept(server_fd, (struct sockaddr *)&info->address, (socklen_t*)&addrlen);

        if (new_socket < 0) {
            perror("accept");
            free(info);
            continue;
        }

        info->socket = new_socket;
        info->user_id = next_user_id++; // Asignar un ID de usuario
        printf("[Servidor] Nuevo cliente! ID: %d, Clientes activos: %d.\n", info->user_id, active_clients + 1);

        // Aumentar el contador de clientes y lanzar el hilo
        pthread_mutex_lock(&clients_mutex);
        active_clients++;
        printf("[Servidor] Nuevo cliente! Clientes activos: %d.\n", active_clients);
        pthread_mutex_unlock(&clients_mutex);

        pthread_t client_thread;
        // Lanzamos un hilo para manejar al cliente de forma concurrente
        if (pthread_create(&client_thread, NULL, handle_client, (void *)info) != 0) {
            perror("pthread_create failed");
            close(new_socket);
            free(info);
            pthread_mutex_lock(&clients_mutex);
            active_clients--;
            pthread_cond_signal(&clients_cond);
            pthread_mutex_unlock(&clients_mutex);
        } else {
            // Esto permite al thread del cliente desvincularse del hilo principal
            // para que sus recursos se limpien automáticamente al terminar.
            pthread_detach(client_thread);
        }
    }

    // El servidor nunca debe llegar aquí en ejecución normal
    return 0;
}

// --- SIMULACIÓN DE LÓGICA DE BD (implementación incompleta por brevedad)

typedef struct {
    int id;
    char product[128];
    int quantity;
    double price;
} Record;

static void rtrim(char *s) {
    size_t n = strlen(s);
    while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r' || s[n-1] == ' ' || s[n-1] == '\t')) {
        s[n-1] = '\0';
        n--;
    }
}

static void ltrim_inplace(char **ps) {
    while (**ps == ' ' || **ps == '\t') (*ps)++;
}

static int parse_record_line(const char *line, Record *rec) {
    // Expect: ID,Producto,Cantidad,Precio
    char copy[512];
    strncpy(copy, line, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';
    char *p = copy;
    rtrim(p);
    if (strlen(p) == 0) return 0;
    if (strncmp(p, "ID,", 3) == 0) return 0; // header

    char *tok;
    // ID
    tok = strtok(p, ","); if (!tok) return -1; rec->id = atoi(tok);
    // Producto
    tok = strtok(NULL, ","); if (!tok) return -1; strncpy(rec->product, tok, sizeof(rec->product) - 1); rec->product[sizeof(rec->product)-1] = '\0';
    // Cantidad
    tok = strtok(NULL, ","); if (!tok) return -1; rec->quantity = atoi(tok);
    // Precio
    tok = strtok(NULL, ","); if (!tok) return -1; rec->price = atof(tok);
    return 1;
}

static void record_to_csv(const Record *rec, char *out, size_t out_size) {
    snprintf(out, out_size, "%d,%s,%d,%.2f\n", rec->id, rec->product, rec->quantity, rec->price);
}

void load_config(char *ip, int *port, int *max_clients, int *backlog) {
    // Implementación simple: carga desde constantes.
    strcpy(ip, "127.0.0.1");
    *port = 8080;
    *max_clients = MAX_CLIENTS;
    *backlog = BACKLOG_QUEUE;
}

static char *read_entire_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    if (sz < 0) { fclose(f); return NULL; }
    fseek(f, 0, SEEK_SET);
    char *buf = (char *)malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    buf[rd] = '\0';
    fclose(f);
    return buf;
}

static int write_all_text(const char *path, const char *text) {
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    size_t len = strlen(text);
    size_t wr = fwrite(text, 1, len, f);
    fclose(f);
    return wr == len;
}

char *execute_query(const char *command, int *is_success) {
    // Soporta:
    //  - SELECT ALL
    //  - SELECT WHERE CAMPO=VALOR  (CAMPO: ID, Producto, Cantidad, Precio)
    *is_success = 0;

    // Copia editable
    char cmd[MAX_COMMAND_LENGTH];
    strncpy(cmd, command, sizeof(cmd) - 1);
    cmd[sizeof(cmd) - 1] = '\0';
    char *pcmd = cmd; ltrim_inplace(&pcmd);

    if (strncmp(pcmd, "SELECT ALL", 10) == 0) {
        char *content = read_entire_file(CSV_FILE_NAME);
        if (!content) {
            char *err = (char *)malloc(64);
            strcpy(err, "ERROR: No se pudo leer el CSV.\n");
            return err;
        }
        *is_success = 1;
        return content;
    }

    if (strncmp(pcmd, "SELECT WHERE", 12) == 0) {
        // Parse: SELECT WHERE CAMPO=VALOR
        char *cond = pcmd + 12;
        ltrim_inplace(&cond);
        char field[32] = {0};
        char value[128] = {0};
        if (sscanf(cond, "%31[^=]=%127s", field, value) != 2) {
            char *err = (char *)malloc(64);
            strcpy(err, "ERROR: Formato de WHERE invalido.\n");
            return err;
        }

        // Quitar posibles comillas simples/dobles
        size_t vlen = strlen(value);
        if ((vlen >= 2) && ((value[0] == '"' && value[vlen-1] == '"') || (value[0] == '\'' && value[vlen-1] == '\''))) {
            value[vlen-1] = '\0';
            memmove(value, value+1, vlen-1);
        }

        FILE *f = fopen(CSV_FILE_NAME, "r");
        if (!f) {
            char *err = (char *)malloc(64);
            strcpy(err, "ERROR: No se pudo abrir el CSV.\n");
            return err;
        }

        size_t cap = 1024; size_t len = 0;
        char *out = (char *)malloc(cap);
        if (!out) { fclose(f); char *err = (char *)malloc(64); strcpy(err, "ERROR: Memoria insuficiente.\n"); return err; }
        out[0] = '\0';

        char line[512];
        while (fgets(line, sizeof(line), f)) {
            Record r; int pr = parse_record_line(line, &r);
            if (pr <= 0) {
                // header u vaco; incluir header siempre
                if (strncmp(line, "ID,", 3) == 0) {
                    size_t l = strlen(line); if (len + l + 1 > cap) { cap *= 2; out = (char *)realloc(out, cap); }
                    memcpy(out + len, line, l); len += l; out[len] = '\0';
                }
                continue;
            }

            int match = 0;
            if (strcasecmp(field, "ID") == 0) {
                match = (r.id == atoi(value));
            } else if (strcasecmp(field, "Producto") == 0) {
                match = (strcmp(r.product, value) == 0);
            } else if (strcasecmp(field, "Cantidad") == 0) {
                match = (r.quantity == atoi(value));
            } else if (strcasecmp(field, "Precio") == 0) {
                match = (fabs(r.price - atof(value)) < 1e-9);
            }

            if (match) {
                char buf[256];
                record_to_csv(&r, buf, sizeof(buf));
                size_t l = strlen(buf); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, buf, l); len += l; out[len] = '\0';
            }
        }
        fclose(f);
        if (len == 0) {
            // Sin resultados (ni header encontrado); devolver mensaje vaco amigable
            char *msg = (char *)malloc(32); strcpy(msg, "OK: 0 filas.\n"); *is_success = 1; return msg;
        }
    *is_success = 1;
        return out;
    }

    char *err = (char *)malloc(64);
    strcpy(err, "ERROR: Formato SELECT no soportado.\n");
    return err;
}

char *perform_modification(const char *command, int *is_success) {
    // Soporta:
    //  - INSERT id,producto,cantidad,precio
    //  - UPDATE ID=<id> SET Campo=Valor
    //  - DELETE ID=<id>
    *is_success = 0;

    char cmd[MAX_COMMAND_LENGTH];
    strncpy(cmd, command, sizeof(cmd) - 1);
    cmd[sizeof(cmd) - 1] = '\0';
    char *pcmd = cmd; ltrim_inplace(&pcmd);

    if (strncmp(pcmd, "INSERT", 6) == 0) {
        // INSERT id,producto,cantidad,precio
        char *args = pcmd + 6; ltrim_inplace(&args);
        int id, qty; double price; char prod[128];
        // Producto no contiene comas en este dataset, asumimos sin comillas
        if (sscanf(args, "%d,%127[^,],%d,%lf", &id, prod, &qty, &price) != 4) {
            char *e = (char *)malloc(64); strcpy(e, "ERROR: Formato INSERT invalido.\n"); return e;
        }
        FILE *f = fopen(CSV_FILE_NAME, "a");
        if (!f) { char *e = (char *)malloc(64); strcpy(e, "ERROR: No se pudo abrir el CSV para escribir.\n"); return e; }
        fprintf(f, "%d,%s,%d,%.2f\n", id, prod, qty, price);
        fclose(f);
    *is_success = 1;
        char *ok = (char *)malloc(64); strcpy(ok, "OK: Fila insertada.\n"); return ok;
    }

    if (strncmp(pcmd, "UPDATE", 6) == 0) {
        // UPDATE ID=<id> SET Campo=Valor
        int id; char field[32]; char value[128];
        if (sscanf(pcmd, "UPDATE ID=%d SET %31[^=]=%127s", &id, field, value) != 3) {
            char *e = (char *)malloc(64); strcpy(e, "ERROR: Formato UPDATE invalido.\n"); return e;
        }
        size_t vlen = strlen(value);
        if ((vlen >= 2) && ((value[0] == '"' && value[vlen-1] == '"') || (value[0] == '\'' && value[vlen-1] == '\''))) {
            value[vlen-1] = '\0';
            memmove(value, value+1, vlen-1);
        }

        FILE *f = fopen(CSV_FILE_NAME, "r");
        if (!f) { char *e = (char *)malloc(64); strcpy(e, "ERROR: No se pudo abrir el CSV.\n"); return e; }

        // Construir nuevo contenido
        size_t cap = 1024; size_t len = 0; int updated = 0;
        char *out = (char *)malloc(cap); if (!out) { fclose(f); char *e = (char *)malloc(64); strcpy(e, "ERROR: Memoria insuficiente.\n"); return e; }
        out[0] = '\0';
        char line[512];
        while (fgets(line, sizeof(line), f)) {
            if (strncmp(line, "ID,", 3) == 0) {
                size_t l = strlen(line); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, line, l); len += l; out[len] = '\0';
                continue;
            }
            Record r; int pr = parse_record_line(line, &r);
            if (pr > 0 && r.id == id) {
                if (strcasecmp(field, "Producto") == 0) {
                    strncpy(r.product, value, sizeof(r.product)-1); r.product[sizeof(r.product)-1] = '\0';
                } else if (strcasecmp(field, "Cantidad") == 0) {
                    r.quantity = atoi(value);
                } else if (strcasecmp(field, "Precio") == 0) {
                    r.price = atof(value);
                }
                char buf[256]; record_to_csv(&r, buf, sizeof(buf));
                size_t l = strlen(buf); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, buf, l); len += l; out[len] = '\0';
                updated = 1; continue;
            }
            // Copiar tal cual
            size_t l = strlen(line); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
            memcpy(out + len, line, l); len += l; out[len] = '\0';
        }
        fclose(f);
        int okw = write_all_text(CSV_FILE_NAME, out);
        free(out);
        if (!okw) { char *e = (char *)malloc(64); strcpy(e, "ERROR: No se pudo escribir el CSV.\n"); return e; }
        *is_success = updated;
        if (updated) { char *ok = (char *)malloc(64); strcpy(ok, "OK: Fila actualizada.\n"); return ok; }
        char *no = (char *)malloc(64); strcpy(no, "OK: 0 filas actualizadas.\n"); return no;
    }

    if (strncmp(pcmd, "DELETE", 6) == 0) {
        // DELETE ID=<id>
        int id;
        if (sscanf(pcmd, "DELETE ID=%d", &id) != 1) {
            char *e = (char *)malloc(64); strcpy(e, "ERROR: Formato DELETE invalido.\n"); return e;
        }
        FILE *f = fopen(CSV_FILE_NAME, "r");
        if (!f) { char *e = (char *)malloc(64); strcpy(e, "ERROR: No se pudo abrir el CSV.\n"); return e; }
        size_t cap = 1024; size_t len = 0; int deleted = 0;
        char *out = (char *)malloc(cap); if (!out) { fclose(f); char *e = (char *)malloc(64); strcpy(e, "ERROR: Memoria insuficiente.\n"); return e; }
        out[0] = '\0';
        char line[512];
        while (fgets(line, sizeof(line), f)) {
            if (strncmp(line, "ID,", 3) == 0) {
                size_t l = strlen(line); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, line, l); len += l; out[len] = '\0';
                continue;
            }
            Record r; int pr = parse_record_line(line, &r);
            if (pr > 0 && r.id == id) { deleted = 1; continue; }
            size_t l = strlen(line); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
            memcpy(out + len, line, l); len += l; out[len] = '\0';
        }
        fclose(f);
        int okw = write_all_text(CSV_FILE_NAME, out);
        free(out);
        if (!okw) { char *e = (char *)malloc(64); strcpy(e, "ERROR: No se pudo escribir el CSV.\n"); return e; }
        *is_success = deleted;
        if (deleted) { char *ok = (char *)malloc(64); strcpy(ok, "OK: Fila eliminada.\n"); return ok; }
        char *no = (char *)malloc(64); strcpy(no, "OK: 0 filas eliminadas.\n"); return no;
    }

    char *e = (char *)malloc(64); strcpy(e, "ERROR: Operacion no soportada.\n"); return e;
}

static void cleanup_resources(void) {
    // Liberar lock si está sostenido
    if (locked_fd >= 0) {
        flock(locked_fd, LOCK_UN);
        close(locked_fd);
        locked_fd = -1;
    }
    // Cerrar socket de servidor si existe
    if (server_fd_global >= 0) {
        close(server_fd_global);
        server_fd_global = -1;
    }
    // Destruir mutexes
    pthread_mutex_destroy(&clients_mutex);
    pthread_mutex_destroy(&lock_status_mutex);
    pthread_cond_destroy(&clients_cond);
}

static void handle_termination_signal(int signum) {
    // Limpieza centralizada y salida
    cleanup_resources();
    exit(0);
}
