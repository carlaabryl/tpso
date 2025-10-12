# Makefile para Micro DB TCP/IP
# Servidor y Cliente de Base de Datos Simple

# Compilador y flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2
LDFLAGS_SERVIDOR = -pthread -lm
LDFLAGS_CLIENTE = 

# Nombres de archivos fuente
SERVIDOR_SRC = servidor.c
CLIENTE_SRC = cliente.c

# Nombres de ejecutables
SERVIDOR_EXE = servidor
CLIENTE_EXE = cliente

# Archivo CSV de ejemplo
CSV_FILE = registros_generados.csv

# Regla por defecto: compilar todo
all: $(SERVIDOR_EXE) $(CLIENTE_EXE)

# Compilar servidor
$(SERVIDOR_EXE): $(SERVIDOR_SRC)
	@echo "Compilando servidor..."
	$(CC) $(CFLAGS) $(SERVIDOR_SRC) -o $(SERVIDOR_EXE) $(LDFLAGS_SERVIDOR)
	@echo "Servidor compilado exitosamente: $(SERVIDOR_EXE)"

# Compilar cliente
$(CLIENTE_EXE): $(CLIENTE_SRC)
	@echo "Compilando cliente..."
	$(CC) $(CFLAGS) $(CLIENTE_SRC) -o $(CLIENTE_EXE) $(LDFLAGS_CLIENTE)
	@echo "Cliente compilado exitosamente: $(CLIENTE_EXE)"

# Crear archivo CSV de ejemplo
csv: $(CSV_FILE)

$(CSV_FILE):
	@echo "Creando archivo CSV de ejemplo..."
	@echo "ID;Producto;Cantidad;Precio" > $(CSV_FILE)
	@echo "1;Laptop;10;899.99" >> $(CSV_FILE)
	@echo "2;Mouse;50;25.50" >> $(CSV_FILE)
	@echo "3;Teclado;30;45.00" >> $(CSV_FILE)
	@echo "4;Monitor;15;299.99" >> $(CSV_FILE)
	@echo "5;Tablet;8;199.99" >> $(CSV_FILE)
	@echo "6;Router;12;79.99" >> $(CSV_FILE)
	@echo "7;Impresora;5;149.99" >> $(CSV_FILE)
	@echo "8;Webcam;20;35.00" >> $(CSV_FILE)
	@echo "9;Auriculares;25;59.99" >> $(CSV_FILE)
	@echo "10;Disco Duro;18;89.99" >> $(CSV_FILE)
	@echo "Archivo CSV creado: $(CSV_FILE)"

# Compilar todo y crear CSV
setup: all csv
	@echo ""
	@echo "=== CONFIGURACIÓN COMPLETA ==="
	@echo "Servidor: $(SERVIDOR_EXE)"
	@echo "Cliente: $(CLIENTE_EXE)"
	@echo "Base de datos: $(CSV_FILE)"
	@echo ""
	@echo "Para ejecutar:"
	@echo "  Servidor: ./$(SERVIDOR_EXE) [N M] [IP PUERTO N M]"
	@echo "  Cliente:  ./$(CLIENTE_EXE) [IP PUERTO]"
	@echo ""

# Limpiar archivos compilados
clean:
	@echo "Limpiando archivos compilados..."
	rm -f $(SERVIDOR_EXE) $(CLIENTE_EXE)
	@echo "Archivos compilados eliminados."

# Limpiar todo (incluyendo CSV)
clean-all: clean
	@echo "Limpiando archivo CSV..."
	rm -f $(CSV_FILE)
	@echo "Todos los archivos generados eliminados."

# Mostrar ayuda
help:
	@echo "=== MAKEFILE MICRO DB ==="
	@echo ""
	@echo "Comandos disponibles:"
	@echo "  make all        - Compilar servidor y cliente"
	@echo "  make servidor   - Compilar solo el servidor"
	@echo "  make cliente    - Compilar solo el cliente"
	@echo "  make csv        - Crear archivo CSV de ejemplo"
	@echo "  make setup      - Compilar todo y crear CSV"
	@echo "  make clean      - Eliminar ejecutables"
	@echo "  make clean-all  - Eliminar ejecutables y CSV"
	@echo "  make help       - Mostrar esta ayuda"
	@echo ""
	@echo "Ejemplos de uso:"
	@echo "  make setup      # Configuración inicial completa"
	@echo "  make clean      # Limpiar después de compilar"
	@echo ""

# Reglas adicionales para compilar individualmente
servidor: $(SERVIDOR_EXE)

cliente: $(CLIENTE_EXE)

# Regla para mostrar información del sistema
info:
	@echo "=== INFORMACIÓN DEL SISTEMA ==="
	@echo "Compilador: $(CC)"
	@echo "Flags: $(CFLAGS)"
	@echo "Servidor flags: $(LDFLAGS_SERVIDOR)"
	@echo "Cliente flags: $(LDFLAGS_CLIENTE)"
	@echo "Sistema operativo: $(shell uname -s)"
	@echo "Arquitectura: $(shell uname -m)"
	@echo ""

# Marcar reglas como no archivos
.PHONY: all clean clean-all help setup csv servidor cliente info
