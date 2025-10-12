#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <signal.h>

#define MAX_BUFFER_SIZE 4096

static int g_sock = -1;

void mostrar_ayuda_cliente(void) {
    printf("\n=== AYUDA - CLIENTE MICRO DB ===\n");
    printf("\nUSO:\n");
    printf("  %s                    - Conectar a servidor local (127.0.0.1:8080)\n", "cliente");
    printf("  %s IP PUERTO          - Conectar a servidor específico\n", "cliente");
    printf("\nEJEMPLOS:\n");
    printf("  %s\n", "cliente");
    printf("  %s 192.168.1.100 9090\n", "cliente");
    printf("\nCOMANDOS DISPONIBLES:\n");
    printf("  HELP                  - Mostrar ayuda detallada del servidor\n");
    printf("  EXIT                  - Desconectar y salir\n");
    printf("\nNOTAS:\n");
    printf("- El servidor debe estar ejecutándose antes de conectar\n");
    printf("- Use Ctrl+C para salir en caso de emergencia\n");
    printf("- Los comandos SQL se envían al servidor para procesamiento\n");
    printf("\n");
}

static void handle_client_signal(int signum) {
    if (g_sock >= 0) {
        close(g_sock);
        g_sock = -1;
    }
    _exit(0);
}

// --- MAIN
int main(int argc, char const *argv[]) {
    int sock = 0;
    struct sockaddr_in serv_addr;
    char buffer[MAX_BUFFER_SIZE] = {0};
    char command[MAX_BUFFER_SIZE];

    const char *ip = "127.0.0.1";
    int puerto = 8080;

    // Manejo de señales para cierre limpio
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_client_signal);
    signal(SIGTERM, handle_client_signal);

    // Validación de parámetros con ayuda automática
    if (argc == 2) {
        // Solo se proporcionó un parámetro (debería ser IP PUERTO)
        printf("ERROR: Parámetros incorrectos.\n");
        printf("Se esperan 0 o 2 parámetros, pero se proporcionó 1.\n\n");
        mostrar_ayuda_cliente();
        return 1;
    } else if (argc > 3) {
        // Demasiados parámetros
        printf("ERROR: Demasiados parámetros.\n");
        printf("Se proporcionaron %d parámetros, pero el máximo es 2.\n\n", argc - 1);
        mostrar_ayuda_cliente();
        return 1;
    } else if (argc == 3) {
        // Validar IP y puerto
        ip = argv[1];
        puerto = atoi(argv[2]);
        
        // Validar puerto
        if (puerto <= 0 || puerto > 65535) {
            printf("ERROR: Puerto inválido: %d\n", puerto);
            printf("El puerto debe estar entre 1 y 65535.\n\n");
            mostrar_ayuda_cliente();
            return 1;
        }
        
        // Validar formato de IP básico (solo verificar que no esté vacío)
        if (strlen(ip) == 0) {
            printf("ERROR: Dirección IP vacía.\n\n");
            mostrar_ayuda_cliente();
            return 1;
        }
    }

    // Creaci�n del socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error de creacion de socket");
        return 1;
    }
    g_sock = sock;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(puerto);

    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Direccion Invalida/No soportada\n");
        return 1;
    }

    // Conexi�n
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Error de conexion al Servidor");
        return 1;
    }

    printf("\n*** Micro DB Cliente ***\n");
    printf("Conectado a %s:%d (Socket %d).\n", ip, puerto, sock);
    printf("Escriba 'HELP' o 'EXIT' para terminar.\n");

    // Leer mensaje inicial de bienvenida del servidor (si lo hay)
    memset(buffer, 0, MAX_BUFFER_SIZE);
    int initread = read(sock, buffer, MAX_BUFFER_SIZE);
    if (initread > 0) {
        printf("<< %s", buffer);
    }

    // Esto permite al cliente ser interactivo y mantenerse en un ciclo de consulta.
    while (1) {
        printf("DB > ");
        if (fgets(command, MAX_BUFFER_SIZE, stdin) == NULL) {
            // Manejo de Ctrl+D o EOF
            break;
        }

        // Eliminar el newline al final
        command[strcspn(command, "\n")] = 0;

        if (strlen(command) == 0) continue;

        if (strncmp(command, "HELP", 4) == 0) {
            printf("Comandos: BEGIN TRANSACTION, COMMIT TRANSACTION, SELECT, INSERT, UPDATE, DELETE, EXIT\n");
            continue;
        }

        if (strncmp(command, "EXIT", 4) == 0) {
            // Enviamos EXIT al servidor antes de terminar
            send(sock, command, strlen(command), 0);
            break;
        }

        // Enviar el comando al servidor
        if (send(sock, command, strlen(command), 0) < 0) {
            perror("Error al enviar datos");
            break;
        }

        // Leer la respuesta del servidor (puede venir en múltiples chunks)
        memset(buffer, 0, MAX_BUFFER_SIZE);
        int total_received = 0;
        int chunk_size = 0;
        
        // Leer el primer chunk
        if ((chunk_size = read(sock, buffer, MAX_BUFFER_SIZE - 1)) <= 0) {
            // Esto permite al cliente manejar el cierre inesperado del servidor (Requisito 7, 10)
            printf("\n[ERROR] Servidor desconectado inesperadamente.\n");
            break;
        }
        
        total_received = chunk_size;
        buffer[total_received] = '\0';
        
        // Verificar si hay marcador de fin de mensaje
        if (strstr(buffer, "---END---") != NULL) {
            // Remover el marcador de fin
            char *end_marker = strstr(buffer, "---END---");
            *end_marker = '\0';
            printf("<< %s", buffer);
        } else if (chunk_size == MAX_BUFFER_SIZE - 1) {
            // Crear un buffer más grande para el contenido completo
            char *full_response = (char *)malloc(MAX_BUFFER_SIZE * 4); // 16KB
            if (full_response) {
                memcpy(full_response, buffer, total_received);
                
                // Continuar leyendo chunks adicionales hasta encontrar el marcador de fin
                while ((chunk_size = read(sock, full_response + total_received, MAX_BUFFER_SIZE - 1)) > 0) {
                    total_received += chunk_size;
                    full_response[total_received] = '\0';
                    
                    // Verificar si encontramos el marcador de fin
                    if (strstr(full_response, "---END---") != NULL) {
                        break;
                    }
                }
                
                // Remover el marcador de fin si existe
                char *end_marker = strstr(full_response, "---END---");
                if (end_marker) {
                    *end_marker = '\0';
                }
                
                printf("<< %s", full_response);
                free(full_response);
            } else {
                printf("<< %s", buffer);
            }
        } else {
            printf("<< %s", buffer);
        }
    }

    close(sock);
    g_sock = -1;
    printf("Desconectado.\n");
    return 0;
}
