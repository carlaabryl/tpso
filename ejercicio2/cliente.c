#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <signal.h>

#define MAX_BUFFER_SIZE 1024

static int g_sock = -1;

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
    int port = 8080;

    // Manejo de señales para cierre limpio
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_client_signal);
    signal(SIGTERM, handle_client_signal);

    // Esto permite al cliente conectarse a la IP y puerto especificados.
    if (argc == 3) {
        ip = argv[1];
        port = atoi(argv[2]);
    } else if (argc != 1) {
        fprintf(stderr, "Uso: %s [IP] [PUERTO]\n", argv[0]);
        return 1;
    }

    // Creaci�n del socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error de creacion de socket");
        return 1;
    }
    g_sock = sock;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

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
    printf("Conectado a %s:%d (Socket %d).\n", ip, port, sock);
    printf("Escriba 'HELP' o 'EXIT' para terminar.\n");

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

        // Leer la respuesta del servidor
        memset(buffer, 0, MAX_BUFFER_SIZE);
        if (read(sock, buffer, MAX_BUFFER_SIZE) <= 0) {
            // Esto permite al cliente manejar el cierre inesperado del servidor (Requisito 7, 10)
            printf("\n[ERROR] Servidor desconectado inesperadamente.\n");
            break;
        }

        printf("<< %s", buffer);
    }

    close(sock);
    g_sock = -1;
    printf("Desconectado.\n");
    return 0;
}
