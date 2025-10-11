# Makefile para generador_datos.c
# Compilador y flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g
TARGET = generador_datos
SOURCE = generador_datos.c

# Regla principal
$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) -o $(TARGET) $(SOURCE)

# Regla para limpiar archivos generados
clean:
	rm -f $(TARGET)
	rm -f registros_generados.csv
	rm -f *.o

# Regla para limpiar recursos IPC (si el programa se queda colgado)
clean-ipc:
	ipcs -m | grep 1234 | awk '{print $$2}' | xargs -r ipcrm -m
	ipcs -s | grep 5678 | awk '{print $$2}' | xargs -r ipcrm -s

# Regla para ejecutar el programa con parámetros por defecto
run: $(TARGET)
	./$(TARGET) 3 100

# Regla para ejecutar con parámetros personalizados
# Uso: make run-custom NUM_GEN=5 TOTAL=200
run-custom: $(TARGET)
	./$(TARGET) $(NUM_GEN) $(TOTAL)

# Regla para debug (con valgrind si está disponible)
debug: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=all ./$(TARGET) 2 50

# Regla para mostrar ayuda
help:
	@echo "Comandos disponibles:"
	@echo "  make          - Compila el programa"
	@echo "  make clean    - Elimina archivos generados"
	@echo "  make clean-ipc- Elimina recursos IPC (memoria compartida y semáforos)"
	@echo "  make run      - Ejecuta el programa con 3 generadores y 100 registros"
	@echo "  make run-custom NUM_GEN=X TOTAL=Y - Ejecuta con parámetros personalizados"
	@echo "  make debug    - Ejecuta con valgrind para detectar memory leaks"
	@echo "  make help     - Muestra esta ayuda"

# Regla por defecto
.PHONY: clean clean-ipc run run-custom debug help
