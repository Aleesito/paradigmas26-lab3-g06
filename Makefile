# Variables de entorno para compatibilidad de Spark con Java 17 en Ubuntu
JAVA_17_HOME = /usr/lib/jvm/java-17-openjdk-amd64
EXPORT_OPTS = --add-exports=java.base/sun.nio.ch=ALL-UNNAMED

# Consolidamos las variables en un solo bloque para pasarlas fácil a los tests
ENV_SETUP = JAVA_HOME=$(JAVA_17_HOME) PATH="$(JAVA_17_HOME)/bin:$$PATH" SBT_OPTS="$(EXPORT_OPTS)"

.PHONY: all run compile test clean

# Regla por defecto al ejecutar solo 'make'
all: run

# Compila y ejecuta
run:
	$(ENV_SETUP) sbt run

# Compila y ejecuta (subscripciones locales)
run-local:
	$(ENV_SETUP) sbt "run --subscription-file data/local_subscriptions.json"

# Solo compila el código
compile:
	sbt compile

# Limpia los archivos compilados y el caché de sbt
clean:
	sbt clean

# Correr los tests verificando que el usuario haya levantado el mock de reddit
test:
	@echo "Verificando conexión con el mock de Reddit en localhost:8123..."
	@if curl -s http://localhost:8123 > /dev/null; then \
		echo "✓ Servidor mock detectado. Corriendo tests..."; \
		$(ENV_SETUP) ./tests.sh; \
	else \
		echo "ERROR: El servidor reddit-mock no está respondiendo."; \
		echo "Por favor, iniciá el servidor mock manualmente en otra terminal (cd reddit-mock && sbt run) y volvé a ejecutar 'make test'."; \
		exit 1; \
	fi