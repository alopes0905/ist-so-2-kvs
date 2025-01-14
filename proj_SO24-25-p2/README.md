# KEY VALUE STORE

Apenas compila em sistema operativo LINUX.

Exemplo de como correr:
```bash
# Inicializar servidor
.src/server/kvs ./src/server/jobs 4 2 /tmp/register_fifo

# Inicializar clientes
.src/client/client c1 /tmp/register_fifo
```
