# Decisión arquitectónica de la sesión 20

## Caso A · Informe de las ocho

latencia_util: Disponible antes de la consulta diaria de las 08:00.
frescura_admitida: Horas; se aceptan datos de la noche anterior.
decision: Procesar los aproximadamente 180 eventos por hora en la ventana 02:00–06:00.
consumidor_continuo: No; el responsable consulta el resultado una vez al día.
conclusion_arquitectura: batch, porque existe una ventana nocturna suficiente.
por_que_no_la_otra: Streaming 24/7 mantendría recursos y operación continua sin aportar valor antes de las 08:00.

1. El responsable de ocupación usa el informe al comenzar la jornada, a las 08:00.
2. Si llega tarde, empezará el día sin la visión consolidada de las doce aulas.
3. Si llega antes, no se toma ninguna acción adicional porque no hay consumidor continuo.

## Caso B · Alerta de aforo

latencia_util: Menos de 2 minutos desde que se produce el evento.
frescura_admitida: Minutos, porque una lectura antigua no representa el aforo actual.
decision: Mantener un flujo continuo que evalúe cada actualización de sensores.
consumidor_continuo: Sí; el sistema de alertas debe observar los eventos durante la actividad.
conclusion_arquitectura: streaming, para reaccionar dentro del límite de 2 minutos.
por_que_no_la_otra: Un batch periódico puede cerrar después de que el exceso de aforo ya haya causado riesgo.

1. El personal de seguridad consume la alerta durante la ocupación de las aulas.
2. Si llega tarde, puede mantenerse una situación de aforo insegura sin intervención.
3. Si llega antes, el personal puede comprobar el aula y actuar preventivamente.
