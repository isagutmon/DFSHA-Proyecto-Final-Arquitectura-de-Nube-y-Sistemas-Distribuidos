# Contribuciones del equipo

## Isabella Gutiérrez

### Infraestructura
- Configuración principal de VM1.
- Despliegue de Control A, etcd-1 y DataNode D1.
- Configuración del entorno asociado al plano de control y al plano de datos en VM1.

### Implementación
- Desarrollo y validación de RF3.
- Lectura y escritura por rango.
- Mecanismo Copy-on-Write.
- Bloqueo exclusivo por archivo.
- Seguridad mediante mTLS y capabilities temporales.
- Mecanismos de consistencia y control de versiones.

### Pruebas
- Pruebas de RF3.
- Prueba de concurrencia con bloqueo exclusivo.
- Pruebas de consistencia por snapshots y versiones.
- Prueba de rechazo de acceso sin capability.
- Prueba de staging ante escritura interrumpida.

### Documentación
- Documentación de RF3.
- Seguridad.
- Consistencia.
- Decisiones arquitectónicas derivadas de la propuesta individual.

---

## Thomas Osorio Passos

### Infraestructura
- Configuración principal de VM2 y VM3.
- Despliegue de Control B, etcd-2, etcd-3, DataNode D2 y DataNode D3.
- Apoyo en la integración distribuida entre los nodos.

### Implementación
- Desarrollo y validación de RF1.
- Desarrollo y validación de RF2.
- Gestión del namespace lógico.
- Transferencia multibloque.
- Replicación con factor R=2.
- Mecanismos de recuperación y reparación de réplicas.

### Pruebas
- Pruebas de RF1.
- Pruebas de RF2.
- Validación de los tres DataNodes.
- Prueba de tolerancia a fallos.
- Reparación de réplicas.
- Pruebas de rendimiento en condición normal y degradada.

### Documentación
- Documentación de RF1 y RF2.
- Evidencias de infraestructura.
- Resultados de rendimiento.
- Evidencias de replicación y recuperación.

---

## Trabajo conjunto

- Diseño de la arquitectura final.
- Integración de Control A y Control B.
- Integración del clúster etcd de tres miembros.
- Integración de D1, D2 y D3.
- Definición del tamaño de bloque de 4 MiB.
- Definición del factor de replicación R=2.
- Validación de RF1, RF2 y RF3.
- Pruebas finales de disponibilidad, seguridad, consistencia y recuperación.
- Informe técnico final.
- Presentación y video de demostración.
