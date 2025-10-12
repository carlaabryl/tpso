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
#include <time.h> // Para usleep y clock_gettime

// --- Constantes y Configuración
#define MAX_COMMAND_LENGTH 512
#define CSV_FILE_NAME "registros_generados.csv"
#define BACKLOG_QUEUE 5 // M clientes en espera (Requisito 1: M)
#define MAX_CLIENTS 5 // N clientes concurrentes (Requisito 1: N)
#define DEFAULT_PORT 8080

// --- Variables Globales de Estado
volatile int clientes_activos = 0;
#define MAX_TOTAL_CLIENTES 1024
static int sockets_clientes[MAX_TOTAL_CLIENTES];
static pthread_mutex_t mutex_sockets_clientes = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_clientes;
int archivo_bloqueado = 0; // Estado: 1 si hay una transacción activa
int bloqueado_por_socket = -1; // Socket del cliente que tiene el lock
pthread_mutex_t mutex_estado_bloqueo; // Mutex para proteger archivo_bloqueado y bloqueado_por_socket
int descriptor_archivo_bloqueado = -1; // Descriptor del archivo bloqueado durante la transacción
static int socket_servidor_global = -1; // socket de escucha global para cierre seguro
static volatile int siguiente_id_usuario = 1;
pthread_cond_t condicion_clientes; // Señaliza disponibilidad de slots de cliente

/* Bandera de terminación segura desde el handler de señales */
static volatile sig_atomic_t stop_requested = 0;

// --- Prototipos
void *handle_client(void *arg);
int try_acquire_lock(int socket_cliente);
void release_lock(int socket_cliente);
void load_config(char *ip, int *puerto, int *max_clientes, int *backlog);
char *execute_query(const char *command, int *is_success);
char *perform_modification(const char *command, int *is_success);
char *mostrar_ayuda_detallada(void);
static char *read_entire_file(const char *path);
static void cleanup_resources(void);
static void handle_termination_signal(int signum);

// Estructura para pasar argumentos al hilo
typedef struct {
    int socket;
    struct sockaddr_in direccion;
    int id_usuario;
} info_cliente;

// --- Funciones de Bloqueo (Transacciones)

int try_acquire_lock(int socket_cliente) {
    int result = 0;

    // Abrimos el archivo en modo lectura/escritura
    int fd = open(CSV_FILE_NAME, O_RDWR);
    if (fd < 0) return -1; // Error de archivo

    // Intentamos un bloqueo exclusivo (LOCK_EX) sin esperar (LOCK_NB)
    if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
        // Bloqueo exitoso
        pthread_mutex_lock(&mutex_estado_bloqueo);
        archivo_bloqueado = 1;
        bloqueado_por_socket = socket_cliente;
        descriptor_archivo_bloqueado = fd; // mantener descriptor abierto para sostener el lock
        pthread_mutex_unlock(&mutex_estado_bloqueo);
        result = 1; // éxito
    } else {
        // Bloqueo fallido (ya bloqueado por otro)
        result = 0; // Fallo
        close(fd);
    }
    return result;
}

void release_lock(int socket_cliente) {
    pthread_mutex_lock(&mutex_estado_bloqueo);
    if (archivo_bloqueado && bloqueado_por_socket == socket_cliente) {
        printf("[SERVIDOR] Liberando lock exclusivo del socket %d...\n", socket_cliente);
        // Liberar el bloqueo sobre el descriptor sostenido
        if (descriptor_archivo_bloqueado >= 0) {
            flock(descriptor_archivo_bloqueado, LOCK_UN);
            close(descriptor_archivo_bloqueado);
            descriptor_archivo_bloqueado = -1;
        }
        archivo_bloqueado = 0;
        bloqueado_por_socket = -1;
        printf("[SERVIDOR] Lock liberado exitosamente. Archivo disponible para otros clientes.\n");
    } else {
        printf("[SERVIDOR] No hay lock activo para el socket %d.\n", socket_cliente);
    }
    pthread_mutex_unlock(&mutex_estado_bloqueo);
}

// --- Manejo de Cliente Concurrente

void *handle_client(void *arg) {
    info_cliente *info = (info_cliente *)arg;
    int socket_cliente = info->socket;
    char buffer[MAX_COMMAND_LENGTH] = {0};
    int transaccion_activa = 0;


    printf("[THREAD %lu] Cliente conectado desde %s:%d\n",
           pthread_self(), inet_ntoa(info->direccion.sin_addr), ntohs(info->direccion.sin_port));

    // Registrar socket en la lista global
    pthread_mutex_lock(&mutex_sockets_clientes);
    for (int i = 0; i < MAX_TOTAL_CLIENTES; ++i) {
        if (sockets_clientes[i] == 0) {
            sockets_clientes[i] = socket_cliente;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_sockets_clientes);

    // Enviar bienvenida con el identificador de usuario asignado
    char welcome[128];
    snprintf(welcome, sizeof(welcome), "Bienvenido. Usted es el Usuario %d. Use HELP para ayuda.\n", info->id_usuario);
    send(socket_cliente, welcome, strlen(welcome), 0);

    // Esto permite al cliente ser interactivo, manteniéndose conectado hasta que decida salir.
    while (1) {
        memset(buffer, 0, MAX_COMMAND_LENGTH);
        int valread = read(socket_cliente, buffer, MAX_COMMAND_LENGTH);

        if (valread <= 0) {
            // Cierre inesperado o desconexión normal
            printf("[THREAD %lu] Cliente desconectado (socket %d).\n", pthread_self(), socket_cliente);
            break;
        }

        char *command = buffer;
        command[strcspn(command, "\n")] = 0; // Eliminar newline

        if (strncmp(command, "EXIT", 4) == 0) {
            // Desconexión normal
            printf("[THREAD %lu] Comando EXIT recibido (socket %d).\n", pthread_self(), socket_cliente);
            break;
        }

        // --- 1. Manejo de Transacciones ---
        else if (strncmp(command, "BEGIN TRANSACTION", 17) == 0) {
            if (try_acquire_lock(socket_cliente)) {
                transaccion_activa = 1;
                send(socket_cliente, "OK: Transaccion iniciada. Lock exclusivo obtenido.\n", 50, 0);
            } else {
                // Si otro cliente lo intenta, debe recibir un error (Requisito 5)
                send(socket_cliente, "ERROR: Transaccion activa. Reintente luego.\n", 44, 0);
            }
        }
        else if (strncmp(command, "COMMIT TRANSACTION", 18) == 0) {
            if (transaccion_activa) {
                release_lock(socket_cliente);
                transaccion_activa = 0;
                send(socket_cliente, "OK: Transaccion confirmada. Lock liberado.\n", 43, 0);
            } else {
                send(socket_cliente, "ERROR: No hay transaccion activa para hacer COMMIT.\n", 52, 0);
            }
        }

        // --- 2. Modificaciones (DML) ---
        else if (strncmp(command, "INSERT", 6) == 0 || strncmp(command, "UPDATE", 6) == 0 || strncmp(command, "DELETE", 6) == 0) {
            // Si hay otra transacción activa, rechazar
            pthread_mutex_lock(&mutex_estado_bloqueo);
            int locked = archivo_bloqueado;
            int locked_by_me = (bloqueado_por_socket == socket_cliente);
            pthread_mutex_unlock(&mutex_estado_bloqueo);

            if (locked && !locked_by_me) {
                send(socket_cliente, "ERROR: Transaccion activa en curso. Reintente luego.\n", 53, 0);
            } else if (!transaccion_activa) {
                send(socket_cliente, "ERROR: Las modificaciones requieren BEGIN TRANSACTION.\n", 55, 0);
            } else {
                int success;
                char *response = perform_modification(command, &success);
                send(socket_cliente, response, strlen(response), 0);
                free(response);
            }
        }

        // --- 3. Consultas (SELECT) ---
        else if (strncmp(command, "SELECT", 6) == 0) {
            pthread_mutex_lock(&mutex_estado_bloqueo);
            int locked = archivo_bloqueado;
            pthread_mutex_unlock(&mutex_estado_bloqueo);

            if (locked) {
                 // Si hay una transacción activa, ningún otro cliente puede realizar consultas (Requisito 5)
                send(socket_cliente, "ERROR: Transaccion activa en curso. Reintente luego.\n", 53, 0);
            } else {
                int success;
                char *response = execute_query(command, &success);

                if (strncmp(command, "SELECT ALL", 10) == 0 && success) {
                    char *content = read_entire_file(CSV_FILE_NAME);
                    if (content) {
                        size_t content_len = strlen(content);
                        if (content_len > 3000) {
                            // Enviar en chunks de 2000 bytes
                            size_t chunk_size = 2000;
                            size_t sent = 0;

                            while (sent < content_len) {
                                size_t remaining = content_len - sent;
                                size_t current_chunk = (remaining > chunk_size) ? chunk_size : remaining;

                                send(socket_cliente, content + sent, current_chunk, 0);
                                sent += current_chunk;

                                // Pequeña pausa para evitar saturar el buffer
                                usleep(1000); // 1ms
                            }

                            // Enviar marcador de fin de mensaje
                            send(socket_cliente, "\n---END---\n", 11, 0);
                            free(content);
                        } else {
                            send(socket_cliente, content, content_len, 0);
                            free(content);
                        }
                    }
                    free(response);
                } else {
                    send(socket_cliente, response, strlen(response), 0);
                    free(response);
                }
            }
        }
        // --- 4. Comando HELP ---
        else if (strncmp(command, "HELP", 4) == 0) {
            char *ayuda = mostrar_ayuda_detallada();
            if (ayuda) {
                send(socket_cliente, ayuda, strlen(ayuda), 0);
                free(ayuda);
            } else {
                send(socket_cliente, "ERROR: No se pudo generar la ayuda.\n", 36, 0);
            }
        }
        
        // --- 5. Comando no reconocido - Mostrar ayuda automáticamente ---
        else {
            char *ayuda = mostrar_ayuda_detallada();
            char *mensaje_error = (char *)malloc(2048 + 100);
            if (mensaje_error && ayuda) {
                snprintf(mensaje_error, 2048 + 100, 
                    "ERROR: Comando no reconocido: '%s'\n\n%s", command, ayuda);
                send(socket_cliente, mensaje_error, strlen(mensaje_error), 0);
                free(mensaje_error);
                free(ayuda);
            } else {
                char *error_simple = "ERROR: Comando no reconocido. Use HELP para ver los comandos disponibles.\n";
                send(socket_cliente, error_simple, strlen(error_simple), 0);
                if (ayuda) free(ayuda);
                if (mensaje_error) free(mensaje_error);
            }
        }
    }

    // --- Cleanup
    if (transaccion_activa) {
        // Manejo de cierre inesperado: si la transacción está activa, debe liberar el lock
        printf("[THREAD %lu] ADVERTENCIA: Cliente desconectado con transaccion activa (socket %d). Liberando lock...\n", 
               pthread_self(), socket_cliente);
        release_lock(socket_cliente);
        printf("[THREAD %lu] Lock liberado exitosamente. Otros clientes pueden realizar operaciones.\n", pthread_self());
    }


    // Eliminar socket de la lista global
    pthread_mutex_lock(&mutex_sockets_clientes);
    for (int i = 0; i < MAX_TOTAL_CLIENTES; ++i) {
        if (sockets_clientes[i] == socket_cliente) {
            sockets_clientes[i] = 0;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_sockets_clientes);

    close(socket_cliente);

    // Esto permite al servidor manejar el conteo de clientes concurrentes (Requisito 1)
    pthread_mutex_lock(&mutex_clientes);
    clientes_activos--;
    printf("[Servidor] Clientes activos: %d.\n", clientes_activos);
    pthread_cond_signal(&condicion_clientes);
    pthread_mutex_unlock(&mutex_clientes);

    free(info);
    pthread_exit(NULL);
}

// --- MAIN
int main(int argc, char *argv[]) {
    int socket_servidor, nuevo_socket;
    struct sockaddr_in direccion;
    int longitud_direccion = sizeof(direccion);
    char ip[16] = "127.0.0.1";
    int puerto = DEFAULT_PORT;
    int config_max_clientes = MAX_CLIENTS;
    int config_backlog = BACKLOG_QUEUE;

    load_config(ip, &puerto, &config_max_clientes, &config_backlog);

    // Validación de parámetros (como antes)
    if (argc == 2) {
        fprintf(stderr, "ERROR: Parámetros incorrectos.\n");
        fprintf(stderr, "Se esperan 0, 2 o 4 parámetros, pero se proporcionó 1.\n\n");
        fprintf(stderr, "USO CORRECTO:\n");
        fprintf(stderr, "  %s                           - Valores por defecto\n", argv[0]);
        fprintf(stderr, "  %s N M                       - Configurar clientes concurrentes y backlog\n", argv[0]);
        fprintf(stderr, "  %s IP PUERTO N M             - Configurar IP, puerto, clientes y backlog\n", argv[0]);
        exit(EXIT_FAILURE);
    } else if (argc > 5) {
        fprintf(stderr, "ERROR: Demasiados parámetros.\n");
        fprintf(stderr, "Se proporcionaron %d parámetros, pero el máximo es 4.\n\n", argc - 1);
        fprintf(stderr, "USO CORRECTO:\n");
        fprintf(stderr, "  %s                           - Valores por defecto\n", argv[0]);
        fprintf(stderr, "  %s N M                       - Configurar clientes concurrentes y backlog\n", argv[0]);
        fprintf(stderr, "  %s IP PUERTO N M             - Configurar IP, puerto, clientes y backlog\n", argv[0]);
        exit(EXIT_FAILURE);
    } else if (argc == 3) {
        config_max_clientes = atoi(argv[1]);
        config_backlog = atoi(argv[2]);
        if (config_max_clientes <= 0) {
            fprintf(stderr, "ERROR: N (clientes concurrentes) debe ser mayor que 0. Valor recibido: %d\n", config_max_clientes);
            exit(EXIT_FAILURE);
        }
        if (config_backlog < 0) {
            fprintf(stderr, "ERROR: M (backlog) debe ser mayor o igual a 0. Valor recibido: %d\n", config_backlog);
            exit(EXIT_FAILURE);
        }
    } else if (argc == 5) {
        strncpy(ip, argv[1], sizeof(ip) - 1);
        ip[sizeof(ip) - 1] = '\0';
        puerto = atoi(argv[2]);
        config_max_clientes = atoi(argv[3]);
        config_backlog = atoi(argv[4]);
        if (strlen(ip) == 0) { fprintf(stderr, "ERROR: Dirección IP vacía.\n"); exit(EXIT_FAILURE); }
        if (puerto <= 0 || puerto > 65535) { fprintf(stderr, "ERROR: Puerto inválido: %d. Debe estar entre 1 y 65535.\n", puerto); exit(EXIT_FAILURE); }
        if (config_max_clientes <= 0) { fprintf(stderr, "ERROR: N (clientes concurrentes) debe ser mayor que 0. Valor recibido: %d\n", config_max_clientes); exit(EXIT_FAILURE); }
        if (config_backlog < 0) { fprintf(stderr, "ERROR: M (backlog) debe ser mayor o igual a 0. Valor recibido: %d\n", config_backlog); exit(EXIT_FAILURE); }
    }


    // Inicialización de mutexes
    pthread_mutex_init(&mutex_clientes, NULL);
    pthread_mutex_init(&mutex_estado_bloqueo, NULL);
    pthread_cond_init(&condicion_clientes, NULL);
    memset(sockets_clientes, 0, sizeof(sockets_clientes));

    // Manejo de señales: evitar caídas por SIGPIPE y limpieza en SIGINT/SIGTERM
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_termination_signal);
    signal(SIGTERM, handle_termination_signal);

    // Registrar limpieza en salida normal
    atexit(cleanup_resources);


    // Creación del socket del servidor
    socket_servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_servidor == 0) { perror("socket failed"); exit(EXIT_FAILURE); }
    socket_servidor_global = socket_servidor;

    int opt = 1;
    if (setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt"); exit(EXIT_FAILURE);
    }
    // Habilitar SO_KEEPALIVE para detectar desconexiones colgadas
    opt = 1;
    if (setsockopt(socket_servidor, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt))) {
        perror("setsockopt SO_KEEPALIVE");
    }

    direccion.sin_family = AF_INET;
    direccion.sin_addr.s_addr = inet_addr(ip);
    direccion.sin_port = htons(puerto);

    if (bind(socket_servidor, (struct sockaddr *)&direccion, sizeof(direccion)) < 0) {
        perror("bind failed"); exit(EXIT_FAILURE);
    }

    if (listen(socket_servidor, config_backlog) < 0) {
        perror("listen"); exit(EXIT_FAILURE);
    }

    printf("Servidor Micro DB escuchando en %s:%d. Max concurrentes (N): %d, Backlog (M): %d.\n", ip, puerto, config_max_clientes, config_backlog);

    // Bucle principal: permanecer a la espera de nuevos clientes
    while (1) {
        // Esperar hasta que haya espacio para otro cliente concurrente (N)
        pthread_mutex_lock(&mutex_clientes);
        while (clientes_activos >= config_max_clientes && !stop_requested) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1; // despertarse cada 1 segundo para comprobar stop_requested
            pthread_cond_timedwait(&condicion_clientes, &mutex_clientes, &ts);
        }
        if (stop_requested) {
            pthread_mutex_unlock(&mutex_clientes);
            break; // salir del bucle principal y limpiar
        }
        pthread_mutex_unlock(&mutex_clientes);

        info_cliente *info = (info_cliente *)malloc(sizeof(info_cliente));
        if (!info) { perror("malloc failed"); continue; }

        // CORRECCIÓN: llamada correcta a accept
        nuevo_socket = accept(socket_servidor, (struct sockaddr *)&info->direccion, (socklen_t *)&longitud_direccion);

        if (nuevo_socket < 0) {
            free(info);
            // Si la causa fue la señal de terminación, salimos limpiamente
            if (stop_requested) {
                break;
            }
            perror("accept");
            continue;
        }

        info->socket = nuevo_socket;
        info->id_usuario = siguiente_id_usuario++; // Asignar un ID de usuario
        printf("[Servidor] Nuevo cliente! ID: %d, Clientes activos: %d.\n", info->id_usuario, clientes_activos + 1);

        // Aumentar el contador de clientes y lanzar el hilo
        pthread_mutex_lock(&mutex_clientes);
        clientes_activos++;
        printf("[Servidor] Nuevo cliente! Clientes activos: %d.\n", clientes_activos);
        pthread_mutex_unlock(&mutex_clientes);

        pthread_t hilo_cliente;
        if (pthread_create(&hilo_cliente, NULL, handle_client, (void *)info) != 0) {
            perror("pthread_create failed");
            close(nuevo_socket);
            free(info);
            pthread_mutex_lock(&mutex_clientes);
            clientes_activos--;
            pthread_cond_signal(&condicion_clientes);
            pthread_mutex_unlock(&mutex_clientes);
        } else {
            pthread_detach(hilo_cliente);
        }
    }

    // Salimos del while principal => stop_requested o error terminal
    printf("[Servidor] Señal de terminación recibida o error. Limpiando recursos...\n");
    cleanup_resources();
    return 0;
}

// --- SIMULACIÓN DE LÓGICA DE BD (implementación incompleta por brevedad)

typedef struct {
    int id;
    char producto[128];
    int cantidad;
    double precio;
} Registro;

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

static int parse_record_line(const char *line, Registro *rec) {
    char copy[512];
    strncpy(copy, line, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';
    char *p = copy;
    rtrim(p);
    if (strlen(p) == 0) return 0;
    if (strncmp(p, "ID;", 3) == 0) return 0; // header

    char *tok;
    tok = strtok(p, ";"); if (!tok) return -1; rec->id = atoi(tok);
    tok = strtok(NULL, ";"); if (!tok) return -1; strncpy(rec->producto, tok, sizeof(rec->producto) - 1); rec->producto[sizeof(rec->producto)-1] = '\0';
    tok = strtok(NULL, ";"); if (!tok) return -1; rec->cantidad = atoi(tok);
    tok = strtok(NULL, ";"); if (!tok) return -1; rec->precio = atof(tok);
    return 1;
}

static void record_to_csv(const Registro *rec, char *out, size_t out_size) {
    snprintf(out, out_size, "%d;%s;%d;%.2f\n", rec->id, rec->producto, rec->cantidad, rec->precio);
}

void load_config(char *ip, int *puerto, int *max_clientes, int *backlog) {
    strcpy(ip, "127.0.0.1");
    *puerto = 8080;
    *max_clientes = MAX_CLIENTS;
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
    *is_success = 0;

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
        size_t content_len = strlen(content);
        if (content_len > 3000) {
            char *chunk_response = (char *)malloc(100);
            snprintf(chunk_response, 100, "OK: Contenido grande (%zu bytes). Enviando en chunks...\n", content_len);
            *is_success = 1;
            return chunk_response;
        }
        *is_success = 1;
        return content;
    }

    if (strncmp(pcmd, "SELECT WHERE", 12) == 0) {
        char *cond = pcmd + 12;
        ltrim_inplace(&cond);
        char field[32] = {0};
        char value[128] = {0};
        if (sscanf(cond, "%31[^=]=%127s", field, value) != 2) {
            char *err = (char *)malloc(64);
            strcpy(err, "ERROR: Formato de WHERE invalido.\n");
            return err;
        }
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
            Registro r; int pr = parse_record_line(line, &r);
            if (pr <= 0) {
                if (strncmp(line, "ID;", 3) == 0) {
                    size_t l = strlen(line); if (len + l + 1 > cap) { cap *= 2; out = (char *)realloc(out, cap); }
                    memcpy(out + len, line, l); len += l; out[len] = '\0';
                }
                continue;
            }

            int match = 0;
            if (strcasecmp(field, "ID") == 0) {
                match = (r.id == atoi(value));
            } else if (strcasecmp(field, "Producto") == 0) {
                match = (strcmp(r.producto, value) == 0);
            } else if (strcasecmp(field, "Cantidad") == 0) {
                match = (r.cantidad == atoi(value));
            } else if (strcasecmp(field, "Precio") == 0) {
                match = (fabs(r.precio - atof(value)) < 1e-9);
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
    *is_success = 0;

    char cmd[MAX_COMMAND_LENGTH];
    strncpy(cmd, command, sizeof(cmd) - 1);
    cmd[sizeof(cmd) - 1] = '\0';
    char *pcmd = cmd; ltrim_inplace(&pcmd);

    if (strncmp(pcmd, "INSERT", 6) == 0) {
        char *args = pcmd + 6; ltrim_inplace(&args);
        int id, qty; double precio; char prod[128];
        if (sscanf(args, "%d;%127[^;];%d;%lf", &id, prod, &qty, &precio) != 4) {
            char *e = (char *)malloc(64); strcpy(e, "ERROR: Formato INSERT invalido.\n"); return e;
        }
        FILE *f = fopen(CSV_FILE_NAME, "a");
        if (!f) { char *e = (char *)malloc(64); strcpy(e, "ERROR: No se pudo abrir el CSV para escribir.\n"); return e; }
        fprintf(f, "%d;%s;%d;%.2f\n", id, prod, qty, precio);
        fclose(f);
    *is_success = 1;
        char *ok = (char *)malloc(64); strcpy(ok, "OK: Fila insertada.\n"); return ok;
    }

    if (strncmp(pcmd, "UPDATE", 6) == 0) {
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

        size_t cap = 1024; size_t len = 0; int updated = 0;
        char *out = (char *)malloc(cap); if (!out) { fclose(f); char *e = (char *)malloc(64); strcpy(e, "ERROR: Memoria insuficiente.\n"); return e; }
        out[0] = '\0';
        char line[512];
        while (fgets(line, sizeof(line), f)) {
            if (strncmp(line, "ID;", 3) == 0) {
                size_t l = strlen(line); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, line, l); len += l; out[len] = '\0';
                continue;
            }
            Registro r; int pr = parse_record_line(line, &r);
            if (pr > 0 && r.id == id) {
                if (strcasecmp(field, "Producto") == 0) {
                    strncpy(r.producto, value, sizeof(r.producto)-1); r.producto[sizeof(r.producto)-1] = '\0';
                } else if (strcasecmp(field, "Cantidad") == 0) {
                    r.cantidad = atoi(value);
                } else if (strcasecmp(field, "Precio") == 0) {
                    r.precio = atof(value);
                }
                char buf[256]; record_to_csv(&r, buf, sizeof(buf));
                size_t l = strlen(buf); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, buf, l); len += l; out[len] = '\0';
                updated = 1; continue;
            }
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
            if (strncmp(line, "ID;", 3) == 0) {
                size_t l = strlen(line); if (len + l + 1 > cap) { cap = (cap + l) * 2; out = (char *)realloc(out, cap); }
                memcpy(out + len, line, l); len += l; out[len] = '\0';
                continue;
            }
            Registro r; int pr = parse_record_line(line, &r);
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

char *mostrar_ayuda_detallada(void) {
    char *ayuda = (char *)malloc(2048);
    if (!ayuda) return NULL;
    
    snprintf(ayuda, 2048,
        "=== AYUDA - MICRO DB ===\n"
        "\n"
        "COMANDOS DE CONSULTA (no requieren transacción):\n"
        "  SELECT ALL                           - Mostrar todos los registros\n"
        "  SELECT WHERE CAMPO=VALOR             - Filtrar registros\n"
        "    Campos disponibles: ID, Producto, Cantidad, Precio\n"
        "    Ejemplos:\n"
        "      SELECT WHERE Producto=Tablet\n"
        "      SELECT WHERE ID=10\n"
        "      SELECT WHERE Cantidad=50\n"
        "      SELECT WHERE Precio=25.99\n"
        "\n"
        "COMANDOS DE TRANSACCIÓN:\n"
        "  BEGIN TRANSACTION                    - Iniciar transacción (obtiene lock exclusivo)\n"
        "  COMMIT TRANSACTION                   - Confirmar transacción (libera lock)\n"
        "\n"
        "COMANDOS DE MODIFICACIÓN (requieren transacción activa):\n"
        "  INSERT id;producto;cantidad;precio   - Insertar nuevo registro\n"
        "    Ejemplo: INSERT 100;Router;5;199.99\n"
        "\n"
        "  UPDATE ID=<id> SET Campo=Valor        - Actualizar registro existente\n"
        "    Ejemplos:\n"
        "      UPDATE ID=10 SET Precio=15.50\n"
        "      UPDATE ID=20 SET Cantidad=42\n"
        "      UPDATE ID=30 SET Producto=Mouse\n"
        "\n"
        "  DELETE ID=<id>                       - Eliminar registro\n"
        "    Ejemplo: DELETE ID=10\n"
        "\n"
        "COMANDOS DE CONTROL:\n"
        "  HELP                                 - Mostrar esta ayuda\n"
        "  EXIT                                 - Desconectar del servidor\n"
        "\n"
        "NOTAS IMPORTANTES:\n"
        "- Las modificaciones requieren BEGIN TRANSACTION antes de ejecutarse\n"
        "- Durante una transacción, otros clientes no pueden hacer consultas ni modificaciones\n"
        "- Use COMMIT TRANSACTION para confirmar los cambios\n"
        "- El formato CSV usa punto y coma (;) como separador\n"
    );
    
    return ayuda;
}

static void cleanup_resources(void) {
    // Liberar lock si está sostenido
    if (descriptor_archivo_bloqueado >= 0) {
        flock(descriptor_archivo_bloqueado, LOCK_UN);
        close(descriptor_archivo_bloqueado);
        descriptor_archivo_bloqueado = -1;
    }
    // Cerrar socket de servidor si existe
    if (socket_servidor_global >= 0) {
        close(socket_servidor_global);
        socket_servidor_global = -1;
    }
    // Destruir mutexes
    pthread_mutex_destroy(&mutex_clientes);
    pthread_mutex_destroy(&mutex_estado_bloqueo);
    pthread_cond_destroy(&condicion_clientes);
}

static void handle_termination_signal(int signum) {
    // Handler minimal: marcar flag y cerrar socket de escucha (close es async-signal-safe)
    stop_requested = 1;
    if (socket_servidor_global >= 0) {
        close(socket_servidor_global);
        socket_servidor_global = -1;
    }
    // Hacer shutdown() de todos los clientes activos para que detecten EOF
    pthread_mutex_lock(&mutex_sockets_clientes);
    for (int i = 0; i < MAX_TOTAL_CLIENTES; ++i) {
        if (sockets_clientes[i] > 0) {
            shutdown(sockets_clientes[i], SHUT_RDWR);
        }
    }
    pthread_mutex_unlock(&mutex_sockets_clientes);
    // NO llamar cleanup_resources() ni exit() desde aquí
}
