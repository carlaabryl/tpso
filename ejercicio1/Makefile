# Makefile para generador_datos.c
# Compilador y flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g
TARGET = generador_datos
SOURCE = generador_datos.c

# Valores por defecto para ejecución
NUM_GEN ?= 3
TOTAL   ?= 100

# Objetivo por defecto
.PHONY: all clean clean-ipc run run-custom debug help
all: $(TARGET)

# Regla principal (usa variables automáticas)
$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) -o $@ $^

# Regla para limpiar archivos generados
clean:
	@rm -f $(TARGET) registros_generados.csv *.o

# Regla para limpiar recursos IPC (si el programa se queda colgado)
# Comprueba que ipcs esté disponible antes de intentar limpiar
clean-ipc:
	@command -v ipcs >/dev/null 2>&1 || { echo "ipcs no disponible, omitiendo clean-ipc"; exit 0; }
	@ipcs -m | grep 1234 | awk '{print $$2}' | xargs -r ipcrm -m || true
	@ipcs -s | grep 5678 | awk '{print $$2}' | xargs -r ipcrm -s || true

# Regla para ejecutar el programa con parámetros por defecto
run: $(TARGET)
	@./$(TARGET) $(NUM_GEN) $(TOTAL)

# Regla para ejecutar con parámetros personalizados
# Uso: make run-custom NUM_GEN=5 TOTAL=200
run-custom: $(TARGET)
	@./$(TARGET) $(NUM_GEN) $(TOTAL)

# Regla para debug (con valgrind si está disponible)
debug: $(TARGET)
	@command -v valgrind >/dev/null 2>&1 \
		&& valgrind --leak-check=full --show-leak-kinds=all ./$(TARGET) 2 50 \
		|| ./$(TARGET) 2 50

# Regla para mostrar ayuda
help:
	@echo "Comandos disponibles:"
	@echo "  make          - Compila el programa (objetivo por defecto 'all')"
	@echo "  make clean    - Elimina archivos generados (rm)"
	@echo "  make clean-ipc- Elimina recursos IPC (si ipcs/ipcrm están disponibles)"
	@echo "  make run      - Ejecuta con NUM_GEN=$(NUM_GEN) TOTAL=$(TOTAL)"
	@echo "  make run-custom NUM_GEN=X TOTAL=Y - Ejecuta con parámetros personalizados"
	@echo "  make debug    - Ejecuta con valgrind si está instalado, si no ejecuta directamente"
	@echo "  make help     - Muestra esta ayuda"
