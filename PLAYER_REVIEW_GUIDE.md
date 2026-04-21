# GuÃ­a de RevisiÃ³n de Jugadores (player_review)

## Resumen

Durante la carga de `dim_player`, **669 jugadores** no pudieron ser enlazados automÃ¡ticamente con precisiÃ³n a travÃ©s de las diferentes fuentes de datos. Estos casos se almacenan en la tabla `player_review` para revisiÃ³n manual.

---

## Scripts Disponibles

### 1. `review_players.py` - Analizar y revisar pendientes

Ver estadÃ­sticas generales sobre los jugadores en review:

```bash
# Ver estadÃ­sticas generales
python review_players.py --stats

# Ver primeros 20 jugadores sin resolver
python review_players.py --unresolved

# Ver primeros 30 mejores candidatos (similitud >= 75%)
python review_players.py --candidates

# Exportar todo a CSV
python review_players.py --export

# Cambiar lÃ­mite de registros mostrados
python review_players.py --unresolved --limit 50
```

**Salida esperada:**
```
[STATS] Jugadores en player_review
  Total: 669
  Sin resolver: 320
  Resueltos: 349

[Por fuente]
  Transfermarkt       - Total: 250 | Sin resolver:  85 | Similitud promedio: 82.5%
  SofaScore           - Total: 200 | Sin resolver: 150 | Similitud promedio: 68.2%
  Understat           - Total: 219 | Sin resolver:  85 | Similitud promedio: 75.1%
```

---

### 2. `query_players.py` - Consultas especÃ­ficas

Hacer anÃ¡lisis detallado:

```bash
# Mostrar distribuciÃ³n de similitud
python query_players.py --distribution

# Jugadores con similitud >= 90% (fÃ¡ciles de resolver)
python query_players.py --high-similarity

# Jugadores sin sugerencia automÃ¡tica (mÃ¡s difÃ­ciles)
python query_players.py --no-suggestion

# Jugadores de una fuente especÃ­fica
python query_players.py --by-source TM          # Transfermarkt
python query_players.py --by-source SofaScore
python query_players.py --by-source Understat

# Buscar un jugador especÃ­fico
python query_players.py --search "Cristiano"
python query_players.py --search "Benzema"
```

---

### 3. `resolve_players.py` - Resolver casos

Resolver los jugadores pendientes:

```bash
# Ver estadÃ­sticas por fuente
python resolve_players.py --stats

# Resolver automÃ¡ticamente casos con similitud >= 75%
python resolve_players.py --auto-accept 75

# Modo interactivo (ir caso a caso)
python resolve_players.py --interactive
```

**En modo interactivo:**
- `A` = Aceptar sugerencia
- `R` = Rechazar (no es el mismo jugador)
- `S` = Saltar para revisar despuÃ©s
- `Q` = Quit (salir)
- `L` = Listar alternativas

---

## Flujo de ResoluciÃ³n Recomendado

### Paso 1: Inspeccionar

```bash
# Ver distribuciÃ³n general
python query_players.py --distribution

# Ver estadÃ­sticas
python review_players.py --stats

# Ver mejores candidatos
python review_players.py --candidates
```

### Paso 2: Resolver fÃ¡ciles

```bash
# Aceptar automÃ¡ticamente similitud >= 75%
python resolve_players.py --auto-accept 75

# Verificar que se resolvieron
python query_players.py --distribution
```

### Paso 3: Revisar manualmente

```bash
# Entrar en modo interactivo para los difÃ­ciles
python resolve_players.py --interactive
```

### Paso 4: Exportar resultados

```bash
# Exportar estado final
python review_players.py --export
```

---

## Significado de Similitud

- **90-100%:** Muy seguro, casi con certeza es el mismo jugador
- **75-89%:** Probable, requiere revisiÃ³n breve
- **50-74%:** Posible, requiere anÃ¡lisis cuidadoso
- **< 50%:** Poco probable, probablemente jugador diferente

---

## Ejemplos de Consultas Directas SQL

Si prefieres hacer consultas directas:

```sql
-- Ver todos los sin resolver
SELECT source_name, source_system, similarity_score, canonical_id_assigned
FROM player_review
WHERE resolved = FALSE
ORDER BY similarity_score DESC;

-- Contar por similitud
SELECT 
    CASE WHEN similarity_score >= 90 THEN '90-100%'
         WHEN similarity_score >= 75 THEN '75-89%'
         WHEN similarity_score >= 50 THEN '50-74%'
         ELSE '< 50%' END as rango,
    COUNT(*) as count
FROM player_review
WHERE resolved = FALSE
GROUP BY rango;

-- Ver especÃ­fico con detalles
SELECT 
    pr.id, pr.source_name, pr.source_system, pr.similarity_score,
    dp.canonical_name, dp.position, dp.nationality
FROM player_review pr
LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
WHERE LOWER(pr.source_name) LIKE '%cristiano%';
```

---

## Campos de player_review

| Campo | Significado |
|-------|-------------|
| `id` | ID de la revisiÃ³n |
| `source_name` | Nombre en la fuente original |
| `source_system` | Sistema (TM, SofaScore, Understat, etc.) |
| `source_id` | ID en ese sistema |
| `suggested_canonical_id` | ID sugerido de dim_player |
| `similarity_score` | 0-100, quÃ© tan similar es |
| `resolved` | TRUE si ya fue resuelto |
| `canonical_id_assigned` | ID final asignado |
| `created_at` | CuÃ¡ndo fue creado |
| `reviewed_at` | CuÃ¡ndo fue revisado |

---

## Estrategia segÃºn el volumen

### Si tienes 50-100 casos:
1. `python review_players.py --candidates` - Ver mejores opciones
2. `python resolve_players.py --auto-accept 80` - Resolver los seguros
3. `python resolve_players.py --interactive` - Revisar el resto

### Si tienes 200+ casos:
1. `python resolve_players.py --auto-accept 85` - Los muy seguros
2. `python resolve_players.py --auto-accept 75` - Los probables
3. `python review_players.py --export` - Exportar el resto a CSV para anÃ¡lisis adicional

### Si tienes 500+ casos:
1. Considerar hacer anÃ¡lisis estadÃ­stico en Python/Pandas
2. Posiblemente reprogramar el algorithm de matching
3. Exportar los irresolubles y entrenar un modelo de ML

---

## Tips

- El primer script (`review_players.py`) es el mÃ¡s Ãºtil para empezar
- Usa `--export` para analizar en Excel/Sheets
- Los casos con `similarity_score >= 85` casi siempre son correctos
- Si hay muchos sin sugerencia, el algoritmo de matching puede mejorarse
- Considera guardar las decisiones manuales para mejorar el matching futuro

