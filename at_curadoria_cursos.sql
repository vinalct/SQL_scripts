DROP TABLE IF EXISTS #levenshteinDistance
DROP TABLE IF EXISTS #treinometroTemp
DROP TABLE IF EXISTS #contentsTemp
DROP TABLE IF EXISTS #coursesTemp
DROP TABLE IF EXISTS #contentDuplicatedInCourses


-- #treinometroTemp: tabela temporária que armazena todos os treinometros e os ids dos cursos.
SELECT * 
INTO #treinometroTemp 
FROM (
	SELECT 
        contentId,
        REPLACE(contentName, '''', '') AS contentName,
        enableyChannelId
	FROM 
        course_assigned
	WHERE 
        REPLACE(contentName, '''', '') LIKE 'Treinômetro%'
) AS treinometroTemp;

SELECT *
INTO #contentsTemp
FROM (
    SELECT 
        items.enableyContentId,
        assigned.enableyChannelId,
        assigned.channelName,
        items.isPublished,
        items.publisherUsername,
        ROW_NUMBER() OVER (PARTITION BY assigned.enableyChannelId ORDER BY assigned.enableyChannelId DESC) AS rowNumber 
    FROM 
        academiavivo_content_items items
    LEFT JOIN
        course_assigned assigned
    ON 
        items.enableyContentId = assigned.contentId
    WHERE 
        originalEnableyChannelId IN (
        'c2f2e268...', -- APERFEIÇOAMENTOS
        '862db0ce...', -- PROCESSOS TERRA
        '1c43fb6b...', -- PROCESSOS ODC
        '57b56925...', -- PARCELAMENTO DE FATURAS
        '976b78dd...', -- MUDANÇA DE TITULARIDADE
        '96d46a17...', -- MUDANÇA DE NÚMERO
        'fdf9e4af...', -- CONTESTAÇÃO DE FATURAS
        '984388c4...', -- APRESENTAÇÃO DE PRODUTOS
        '1108844a...', -- ALTERAÇÃO CADASTRAL
        'e573960c...', -- 06 - MÓDULO PRATICANDO
        'fd0b12c7...', -- 05 - TÉCNICA DE VENDAS / CROSS
        '4b4f1502...', -- 02 - FINAL PADRÃO
        'dde55b62...', -- PORTABILIDADE
        '990016f0...', -- ESCOPOS DE GRUPOS
        '6c69b67d...', -- PROCESSOS GERAIS
        '5e4c0c9d....', -- SUPORTE TÉCNICO
        'f805239f...', -- RETENÇÃO/FIDELIZAÇÃO
        '04a3aca2...', -- ALTAS E RENTABILIZAÇÃO
        '965a0f82...', -- MUDANÇA DE ENDEREÇO
        '0e2cc61b...', -- FATURAS / CONTAS
        '525633b3...', -- APRESENTAÇÃO DE SISTEMAS
        'cdd05fa6...', -- 03 - COMPORTAMENTAIS
        'eada3863...', -- 04 - OBRIGATÓRIO (IN)
        '5bbb756b...' -- 01 - BOAS VINDAS
        )
) AS contentsTemp;


SELECT *
INTO #coursesTemp 
FROM (
    SELECT 
        --Seleciona colunas que irao para o relatorio.
        CASE
            WHEN 
                ISDATE(LEFT(cv.creationDate, 10)) = 1
            THEN 
                CONVERT(DATE, LEFT(cv.creationDate, 10), 120)
            ELSE NULL
        END AS dataCriacao,
        ca.enableyChannelId,
        ca.channelExternalId, 
        ca.channelType,
        --retira aspas simples dos dados da coluna.
        REPLACE(ca.channelName, '''','') AS channelName, 
        cv.description, 
        cv.durationStr, 
        cv.hasInstructorNotification,
        REPLACE(REPLACE(cat.profileExternalIds, '''', ''), ';', ', ') AS segmentacaoCatalogo,
        cv.creatorUserId AS idCriador,
        --concatena o primeiro nome com o segundo nome.
        CONCAT(ub.firstName, ' ', ub.lastName) AS nomeCriador,
        cv.isPublishedToCatalog,
        cv.iconHref,
        ROW_NUMBER() OVER (PARTITION BY ca.enableyChannelId ORDER BY contentId) AS rn
    FROM 
        --base de cursos.
        course_assigned ca
    LEFT JOIN 
        --base que contem informacoes extras dos cursos.
        academiavivo_channels_view cv
    ON 
        ca.enableyChannelId = cv.enableyChannelId
    LEFT JOIN 
        --base que contem a segmentacao atual dos cursos.
        catalog_visible_entities cat
    ON
        ca.enableyChannelId = cat.enableyChannelId
    LEFT JOIN
        --base de usuarios.
        users_base ub
    ON 
        cv.creatorUserId = ub.externalId
    WHERE 
        --condicao que traz os cursos criados pelas matriculas abaixo. Todos os membros do atendimento, incluindo a GP.
        (cv.creatorUserId IN (
        '...MATRICULAS...')
    OR
        --junto com a condicao anterior, aqui traz os cursos que não tem a informação de criador do curso.
        (cat.profileExternalIds LIKE '%PROPRIO__ATENDIMENTO' AND cv.creatorUserId = ''))
    AND
        --retira as aspas simples e traz os cursos que não são trilhas.
        REPLACE(ca.channelName, '''','') NOT LIKE 'TR %'
    AND
        --traz os registros que não são curso master ou turma.
        ca.masterCourseExternalId = 'NULL'
) AS coursesTemp;

SELECT * 
INTO #contentDuplicatedInCourses
FROM (
    SELECT
        ct.enableyContentId AS contentID,
        c.enableyChannelId AS ID1,
        c.channelName AS courseName,
        ROW_NUMBER() OVER (PARTITION BY ct.enableyContentId ORDER BY c.enableyChannelId DESC) AS rn
    FROM
        #contentsTemp ct 
    JOIN
        #coursesTemp c
    ON 
        ct.enableyChannelId = c.enableyChannelId
    WHERE 
        c.rn = 1

) AS contentDuplicated;


SELECT
    c.channelName,
    c.enableyChannelId,
    cd.enableyContentId,
    cd.enableyChannelId,
        CASE 
        WHEN EXISTS(
            SELECT 1
            FROM #contentsTemp cd
            WHERE cd.enableyChannelId = c.enableyChannelId AND cd.enableyContentId = ''
        ) THEN 1
            ELSE 0
        END AS contentAddedWrongCheck
FROM 
    #coursesTemp c
LEFT JOIN 
    #contentsTemp cd 
ON 
    c.enableyChannelId = cd.enableyChannelId
WHERE c.rn = 1

SELECT * 
INTO #levenshteinDistance
FROM (
    SELECT
        c.channelExternalId AS ID1,
        c2.channelExternalId AS ID2,
        c.channelName AS name1, 
        c2.channelName AS name2,
        CASE
            WHEN TRIM(c.channelName) = TRIM(c2.channelName) THEN 1
        END AS distance
    FROM
        #coursesTemp c 
    CROSS JOIN 
        #coursesTemp c2
    WHERE
        c.channelExternalId < c2.channelExternalId

) AS levenshteinDistance;



SELECT 
    --Seleciona colunas que irao para o relatorio.
    c.dataCriacao,
    c.enableyChannelId,
    c.channelExternalId, 
    c.channelType,
    --retira aspas simples dos dados da coluna.
    c.channelName, 
    c.description, 
    c.durationStr,
    c.isPublishedToCatalog,
    c.segmentacaoCatalogo,
    c.idCriador,
    c.hasInstructorNotification,
    c.nomeCriador,

    --Verifica na base de certificados se os curso tem certificado. Se sim, retorna 1. Se não, retorna 0.
    CASE
        WHEN EXISTS (
        SELECT 1
        FROM users_certs uc
        WHERE uc.enableyChannelId = c.enableyChannelId
        ) THEN 1
            ELSE 0
        END AS certsExistsCheck,

    -- Caso que verifica se há treinometro no curso.
    CASE
        WHEN EXISTS(
        SELECT 1
        FROM #treinometroTemp tr
        WHERE c.enableyChannelId = tr.enableyChannelId
        ) THEN 1
            ELSE 0
        END AS treinoExistsCheck,

    -- Caso que verifica se o treinometro foi adicionado de forma correta (clonando da estante).
    CASE
        WHEN EXISTS(
            SELECT 1
            FROM #treinometroTemp tr
            WHERE c.enableyChannelId = tr.enableyChannelId AND (
                tr.contentId = '6fd9fad8...' OR -- ID Treinometro Assincrono
                tr.contentId = '9570540e...' OR --ID Treinometro Sincrono
                tr.contentId = '6ef09428...' --ID Treinometro Live
            )
        ) THEN 1
            ELSE 0
        END AS treinoAddedWrongCheck,

    -- Caso que verifica se o conteúdo do curso NÃO está publicado.
    CASE
        WHEN EXISTS(
            SELECT 1
            FROM #contentsTemp ct
            WHERE ct.enableyChannelId = c.enableyChannelId AND ct.isPublished = 'false'
        ) THEN 1
            ELSE 0
        END AS contentNotPublishedCheck,
    
    CASE 
       WHEN EXISTS(
            SELECT 1
            FROM #contentsTemp ct
            WHERE ct.enableyChannelId = c.enableyChannelId AND ct.isPublished = 'false' AND ct.publisherUsername <> ''
       ) THEN (SELECT CONCAT(ub.firstName, ' ', ub.lastName) AS nomeEditor FROM users_base ub JOIN #contentsTemp ct ON ub.username = ct.publisherUsername)
        ELSE NULL
        END AS lastPublisherName,

    CASE 
        WHEN EXISTS(
            SELECT 1
            FROM #contentDuplicatedInCourses cd
            WHERE cd.ID1 = c.enableyChannelId AND cd.rn <> 1
        ) THEN 1
            ELSE 0
        END AS contentInMoreThan1course,

    CASE 
        WHEN 
            c.enableyChannelId NOT IN (
                SELECT
                    ct.enableyChannelId
                FROM 
                    #contentsTemp ct 
                JOIN 
                    #coursesTemp c 
                ON 
                    ct.enableyChannelId = c.enableyChannelId
                )
         THEN 1
            ELSE 0
        END AS contentAddedWrongCheck,

    -- Caso que verifica se a descrição do curso está de acordo com o padrão.
    CASE
        WHEN (
            LEN(c.description) - LEN(REPLACE(c.description, '|', '')) <> 3
        ) THEN 1
            ELSE 0
        END AS descriptionWrongCheck,

    -- Caso que verifica se a formato da duração está de acordo com o padrão. 
    CASE 
        WHEN 
            c.durationStr <> '' AND 
            LEN(c.durationStr) = 5 AND
            SUBSTRING(c.durationStr, 3, 1) = ':' AND
            ISNUMERIC(SUBSTRING(c.durationStr, 1, 2)) = 1 AND 
            ISNUMERIC(SUBSTRING(c.durationStr, 4, 2)) = 1
            THEN 0
            ELSE 1
        END AS durationFormatCheck,

    CASE 
        WHEN 
			c.segmentacaoCatalogo = 'NULL' OR
            c.segmentacaoCatalogo = '' OR (
                LEN(REPLACE(REPLACE(c.segmentacaoCatalogo, 'PROPRIO__ATENDIMENTO,', ''), 'PROPRIO__ATENDIMENTO', '')) +
                LEN(REPLACE(REPLACE(c.segmentacaoCatalogo, 'PROPRIO__GR_CAPACITAÇÃO,', ''), 'PROPRIO__GR_CAPACITAÇÃO', '')) +
                LEN(REPLACE(REPLACE(c.segmentacaoCatalogo, 'ALIADO__GP,', ''), 'ALIADO__GP', '')) +
                LEN(REPLACE(REPLACE(c.segmentacaoCatalogo, 'ALIADO__ATENDIMENTO,', ''), 'ALIADO__ATENDIMENTO', '')) != 0 
            ) OR (
                (LEN(c.segmentacaoCatalogo) - LEN(REPLACE(c.segmentacaoCatalogo, 'PROPRIO__ATENDIMENTO', ''))) / LEN('PROPRIO__ATENDIMENTO') != 1
                OR
                (LEN(c.segmentacaoCatalogo) - LEN(REPLACE(c.segmentacaoCatalogo, 'PROPRIO__GR_CAPACITAÇÃO', ''))) / LEN('PROPRIO__GR_CAPACITAÇÃO') != 1
                OR
                (LEN(c.segmentacaoCatalogo) - LEN(REPLACE(c.segmentacaoCatalogo, 'ALIADO__GP', ''))) / LEN('ALIADO__GP') != 1
                OR
                (LEN(c.segmentacaoCatalogo) - LEN(REPLACE(c.segmentacaoCatalogo, 'ALIADO__ATENDIMENTO', ''))) / LEN('ALIADO__ATENDIMENTO') != 1
            )
            THEN 1
            ELSE 0
        END AS segmentoCheck,

    CASE
        WHEN
            c.iconHref = '' OR
            c.iconHref = 'NULL' OR
            c.iconHref IS NULL
            THEN 1
            ELSE 0
        END AS capaNotExistsCheck,

    CASE
        WHEN EXISTS(
            SELECT 1
            FROM #levenshteinDistance ld 
            WHERE c.channelExternalId = ld.ID1
            AND ld.distance = 1
        )
            THEN 1
            ELSE 0
        END AS duplicatedChannelNameCheck,
        
    CASE 
        WHEN 
            DATEDIFF(DAY, c.dataCriacao, CAST(GETDATE() AS DATE)) > 15 AND 
            c.channelName LIKE '%teste%' 
            THEN 1 
            ELSE 0 
        END AS testCourseMoreThan15days

    FROM 
        #coursesTemp c 
    WHERE 
        c.rn = 1


DROP TABLE #levenshteinDistance
DROP TABLE #treinometroTemp
DROP TABLE #contentsTemp
DROP TABLE #coursesTemp
DROP TABLE #contentDuplicatedInCourses
