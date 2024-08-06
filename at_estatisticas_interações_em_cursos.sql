WITH atendimentoCourses AS (
    SELECT
        cv.channelId AS channelId,
        ROW_NUMBER() OVER (PARTITION BY cv.channelId ORDER BY cv.channelId DESC) AS rn
    FROM  
        academiavivo_channels_view cv
    LEFT JOIN
        catalog_visible_entities cat
    ON 
        cv.enableyChannelId = cat.enableyChannelId
    LEFT JOIN
        course_assigned ca 
    ON 
        cv.enableyChannelId = ca.enableyChannelId
    WHERE 
        --condicao que traz os cursos criados pelas matriculas abaixo. Todos os membros do atendimento, incluindo a GP.
        (cv.creatorUserId IN (
        '...MATRICULAS...')
    OR
        --junto com a condicao anterior, aqui traz os cursos que não tem a informação de criador do curso.
        (cat.profileExternalIds LIKE '%PROPRIO__ATENDIMENTO' AND cv.creatorUserId = ''))
    AND
        --retira as aspas simples e traz os cursos que não são trilhas.
        REPLACE(cv.channelName, '''','') NOT LIKE 'TR %'
    AND
        --traz os registros que não são curso master ou turma.
        ca.masterCourseExternalId = 'NULL'
),

-- Filtra os dados excluindo os 5% superiores e os 5% inferiores (outliers), 
-- com base no totalTimeInSec.
filteredData AS (
    SELECT
        channelExternalId,
        channelName,
        channelDuration,
        progress,
        totalTimeInSec
    FROM (
        SELECT
            channelExternalId,
            channelName,
            channelDuration,
            progress,
            totalTimeInSec,
            -- Numera as linhas com base na coluna totalTimeInSec.
            ROW_NUMBER() OVER (ORDER BY CAST(totalTimeInSec AS INT)) AS rn,
            -- Conta o total de registros.
            COUNT(*) OVER () AS cnt
        FROM
            user_course
        WHERE 
            -- considera apenas usuários com curso concluído.
            progress = 100
    ) AS sub
    WHERE 
        -- Exclui os 5% superiores e inferiores.
        rn > CAST(0.05 * cnt AS INT) AND rn <= CAST( 0.95 * cnt AS INT)  
    
),

-- Calcula estatísticas principais para cada curso.
courseStats AS (
    SELECT
        channelExternalId,
        channelName,
        channelduration,
        -- Calcula o tempo médio gasto (em minutos).
        AVG(CAST(totalTimeInSec AS INT)) / 60 AS meanTimeSpend,
        -- Calcula o desvio de padrão do tempo gasto (em minutos).
        STDEV(CAST(totalTimeInSec AS INT)) / 60 AS stDevTimeSpend,
        -- Conta o número total de interações filtradas.
        COUNT(*) AS filteredTotUsersInteration,
        -- Conta o número de usuários que completaram o curso.
        COUNT(CASE WHEN progress = 100 THEN 1 END) AS filteredTotUsersCompleted,
        -- Conta o número de usuários que NÃO completaram o curso.
        COUNT(CASE WHEN progress <> 100 THEN 1 END) AS filteredTotUsersNotCompleted,
        -- Calcula o tempo mínimo gasto (em minutos).
        MIN(CAST(totalTimeInSec AS INT)) / 60 AS filteredMinTimeSpend,
        -- Calcula o tempo máximo gasto (em minutos).
        MAX(CAST(totalTimeInSec AS INT)) / 60 AS filteredMaxTimeSpend
    FROM
        filteredData
    WHERE 
        channelExternalId IN (SELECT channelId FROM atendimentoCourses WHERE rn = 1)
    GROUP BY 
        channelExternalId, channelName, channelduration
),

-- Calcula os quartis (Q2 = 50% dos usuários, e Q3 = 75% dos usuários) para cada curso.
percentiles AS (
    SELECT DISTINCT
        channelExternalId,
        -- Calcula o percentil 50 (mediana).
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(totalTimeInSec AS INT)) OVER (PARTITION BY channelExternalId) / 60, 1) AS filtQ2,
        -- Calcula o percentil 75.
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(totalTimeInSec AS INT)) OVER (PARTITION BY channelExternalId) / 60, 1) AS filtQ3
    FROM
        filteredData
    WHERE 
        channelExternalId IN (SELECT channelId FROM atendimentoCourses WHERE rn = 1)
),

-- Conta o total de usuários, sem o filtro de outliers, e a taxa de conclusão.
userCounts AS (
    SELECT
        channelExternalId,
        -- Conta o número total de usuários que interagiram.
        COUNT(*) AS totNumUsers,
        -- Conta o número de usuários que completaram o curso.
        COUNT(CASE WHEN progress = 100 THEN 1 END) AS totUserCompleted,
        -- Conta o número de usuários que não completaram o curso.
        COUNT(CASE WHEN progress <> 100 THEN 1 END) AS totUserNotCompleted,
        -- Calcula a porcentagem de usuários que completaram o curso em menos de 60 segundos, limita em 2 casas decimais e altera o separador por ','.
        FORMAT(COUNT(CASE WHEN progress = 100 AND CAST(totalTimeInSec AS INT) <= 60 THEN 1 END) * 1.0 / COUNT(*) * 100, 'N2', 'de-DE') AS percCompletedUnder60Sec,
        -- Calcula a taxa de conclusão, limita em 2 casas decimais e altera o separador por ','.
        FORMAT(COUNT(CASE WHEN progress = 100 THEN 1 END) * 1.0 / COUNT(*) * 100, 'N2', 'de-DE') AS completionRate
    FROM
        user_course
    WHERE 
        channelExternalId IN (SELECT channelId FROM atendimentoCourses WHERE rn = 1)
    GROUP BY 
        channelExternalId
)

-- Combina os resultados de todas as CTEs.
SELECT
    cs.channelExternalId,
    cs.channelName,
    cs.channelDuration,
    cs.meanTimeSpend,
    cs.stDevTimeSpend,
    -- Calculate a margem de erro.
    ROUND((1.96 * cs.stDevTimeSpend / SQRT(cs.filteredTotUsersInteration)), 2) AS marginOfError,
    uc.percCompletedUnder60Sec,
    cs.filteredMinTimeSpend,
    cs.filteredMaxTimeSpend,
    p.filtQ2,
    p.filtQ3,
    uc.completionRate,
    cs.filteredTotUsersInteration,
    cs.filteredTotUsersCompleted,
    cs.filteredTotUsersNotCompleted,
    uc.totNumUsers,
    uc.totUserCompleted,
    uc.totUserNotCompleted
FROM
    courseStats cs 
JOIN
    userCounts uc 
ON 
    cs.channelExternalId = uc.channelExternalId
JOIN
    percentiles p 
ON
    cs.channelExternalId = p.channelExternalId
WHERE 
    -- Filtra registro onde o curso teve pelo menos 50 interações.
    uc.totNumUsers >= 50