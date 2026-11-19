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

## Extras del indicador de TradingView (UT Bot + LinReg + S/R + panel)
Suman hasta +20 puntos a la calidad (el tope sigue siendo 100). Todos se pueden apagar con variables.
- **UT Bot** (trailing stop ATR): +4 si va a favor de la operacion, +2 mas si giro en las ultimas 3 velas. `LQ_UT_REQUIRE=true` lo convierte en filtro duro.
- **LinReg** (cierre suavizado vs su media de 7): +3 si el impulso va a favor.
- **S/R**: +3 si la zona coincide con el maximo/minimo de 100-500 velas.
- **Tendencia multi-TF** (EMA9 vs EMA21 en 3 TF superiores): +2 por TF a favor, -1 por TF en contra (de -4 a +6). `LQ_HTF_MIN_ALIGNED=2` exige al menos 2 a favor.
- **Maximo/minimo de ayer**: +2 si la zona coincide.
- **Longevidad**: el mensaje indica cuanto tiempo lleva la zona sin barrerse.
- **Panel** en el mensaje: volatilidad, ATR, volumen, VWAP, MACD, Stoch, RSI, UT, LinReg, tendencias y nivel de ayer.
Como suman puntos, conviene subir `LQ_MIN_QUALITY` (por ejemplo a 72) para mantener un ritmo parecido de senales.

## Gestion
SL: mas alla del extremo del barrido (+ buffer ATR). TP: siguiente zona de liquidez opuesta importante
(o R:R fijo). Salida: TP, SL o tiempo maximo. Cada senal abre una posicion virtual y el bot avisa al salir.

## Resumen diario
Operaciones, % acierto, esperanza por operacion, profit factor, drawdown maximo, por tipo / timeframe / lado.

## Despliegue
Segundo servicio en Railway sobre el MISMO repo: Settings -> Custom Start Command: `python liquidez_bot.py`
Variables propias del servicio (ver .env.liquidez.example). Comparte requirements.txt con el otro bot.
