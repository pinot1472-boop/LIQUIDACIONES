# Bot de Liquidez (independiente) - BingX -> Telegram

Entra solo cuando el precio esta sobre una zona de liquidez importante y muestra fuerza.

## Setups
- **SOUP (Turtle Soup)**: la vela barre la zona (maximos/minimos iguales) y cierra de vuelta dentro, con volumen y mecha de rechazo.
- **SOUP+1 (Plus One)**: la vela barre y cierra fuera; la siguiente recupera el nivel con vela a favor.
- **REBOTE**: rechazo en la zona sin barrido (mecha >= 50% + volumen). Setup mas debil (-10 de calidad).

## Zonas (arriba, abajo e internas)
Cumulos de pivotes sin barrer, puntuados 0-100: toques (30) + recencia (10) + volumen en pivotes (15)
+ confluencia con liquidaciones estimadas (30) + extremo de 20 velas / zona externa (15).
Se etiquetan como externa alta, externa baja o interna (medio del rango).

## Liquidaciones estimadas
No hay datos publicos de posiciones reales. Se usa el modelo habitual de los heatmaps: volumen por vela
repartido en apalancamientos (10/25/50/100x), precio de liquidacion = entrada * (1 -/+ 0.95/L),
descartando niveles que el precio ya consumio. Es una estimacion, no un dato real.

## Gestion
SL: mas alla del extremo del barrido (+ buffer ATR). TP: siguiente zona de liquidez opuesta importante
(o R:R fijo). Salida: TP, SL o tiempo maximo. Cada senal abre una posicion virtual y el bot avisa al salir.

## Resumen diario
Operaciones, % acierto, esperanza por operacion, profit factor, drawdown maximo, por tipo / timeframe / lado.

## Despliegue
Segundo servicio en Railway sobre el MISMO repo: Settings -> Custom Start Command: `python liquidez_bot.py`
Variables propias del servicio (ver .env.liquidez.example). Comparte requirements.txt con el otro bot.
