-- CONSULTA QUE CONSOLIDA AS AVALIAÇÕES QUE ESTÃO NOS CURSOS, E APRESENTA AS TRILHAS QUE CONTÉM ESSES CURSOS.

-- Serie de CTEs que consolida tabelas que posteriormente serão cruzadas.
-- tabela: questions - reuni todos os conteúdos, dos workspaces do atendimento, que tem nome 'Avalia'.
WITH questions AS (
        SELECT 
        enableyContentId,
        REPLACE(name, '"','') AS contentNameQuestions
    FROM 
        academiavivo_content_items
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
        'dde55b62...', -- PORTABILIDADE
        '990016f0...', -- ESCOPOS DE GRUPOS
        '6c69b67d...', -- PROCESSOS GERAIS
        '5e4c0c9d...', -- SUPORTE TÉCNICO
        'f805239f...', -- RETENÇÃO/FIDELIZAÇÃO
        '04a3aca2...', -- ALTAS E RENTABILIZAÇÃO
        '965a0f82...', -- MUDANÇA DE ENDEREÇO
        '0e2cc61b...', -- FATURAS / CONTAS
        '525633b3...', -- APRESENTAÇÃO DE SISTEMAS
        'cdd05fa6...', -- 03 - COMPORTAMENTAIS
        'eada3863...', -- 04 - OBRIGATÓRIO (IN)
        '5bbb756b...' -- 01 - BOAS VINDAS
        ) AND
        REPLACE(name, '"','') LIKE '%Avalia%'
),

-- tabela: courses - reuni todos os cursos de atendimento e seus conteúdos.
-- ca = course_assigned
-- cv = academiavivo_channels_view
courses AS (
    SELECT 
      ca.contentId,
      REPLACE(ca.contentName, '''','') AS contentName,
      REPLACE(ca.channelName, '''','') AS channelName,
      REPLACE(ca.channelExternalId, '''','') AS channelExternalId,
      cv.creatorUserId
    FROM
        course_assigned ca 
    LEFT JOIN
        academiavivo_channels_view cv 
    ON 
        ca.enableyChannelId = cv.enableyChannelId
    LEFT JOIN
        catalog_visible_entities cat 
    ON 
        ca.enableyChannelId = cat.enableyChannelId
    WHERE
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
),

-- tabela: trails - reuni todas as trilhas do atendimento e seus conteúdos.
-- ca = course_assigned
-- cv = academiavivo_channels_view
trails AS (
    SELECT 
        ca.contentId,
        REPLACE(ca.contentName, '''','') AS contentName,
        REPLACE(ca.channelName, '''','') AS channelName,
        REPLACE(ca.channelExternalId, '''','') AS channelExternalId,
        cv.creatorUserId
    FROM
        course_assigned ca
    LEFT JOIN 
        academiavivo_channels_view cv 
    ON
        ca.enableyChannelId = cv.enableyChannelId
    WHERE
        -- Filtra somente cursos (trilhas) que não são turmas.
		ca.masterCourseExternalId = 'NULL' 
		AND
    -- É aplicado duas condicoes: 
    -- 1. filtra trilhas que foram criadas pelas matriculas dentro do IN.
	(cv.creatorUserId 
		IN (
            '...MATRICULAS...'
    -- 2. e traz trilhas que nao há registro de criador.
	) OR cv.creatorUserId = '') AND
    -- Filtra cursos que comecam com o nome TR.
	REPLACE(ca.channelName, '''', '') LIKE 'TR %'
),

-- tabela: coursesWithQuestions - realiza o cruzamento para descobrir quais cursos contém as avaliações.
-- q = questions
-- c = courses
coursesWithQuestions AS (
    SELECT
        q.enableyContentId,
        c.channelExternalId
    FROM
        questions q
    LEFT JOIN
        courses c
    ON 
        q.enableyContentId = c.contentId
),

-- tabela: coursesContents - reuni os conteúdos dos cursos, tirando as avaliações e treinometro, que consta
-- na CTE anterior (courseWithQuestions).
coursesContents AS (
    SELECT 
        contentId,
        channelName,
        contentName,
        channelExternalId
    FROM
        courses
    WHERE
        channelExternalId IN (
            SELECT channelExternalId FROM coursesWithQuestions
            )
        AND
        contentName NOT LIKE '%Avalia%' 
        AND
        contentName NOT LIKE '%Treinômetro%'
)

-- por fim, realiza o cruzamento entre as CTEs.
SELECT 
    t.channelExternalId AS idTrilha,
    t.channelName AS nomeTrilha,
    t.contentName AS conteudoTrilha,
    cc.contentName AS conteudoCurso,
    cc.channelName AS nomeCurso,
    cwq.enableyContentId AS idAvaliacao,
    q.contentNameQuestions AS nomeAvaliacao
FROM
    trails t
INNER JOIN
    coursesContents cc 
ON
    t.contentId = cc.contentId
LEFT JOIN
    coursesWithQuestions cwq
ON
    cc.channelExternalId = cwq.channelExternalId
LEFT JOIN
    questions q 
ON
    cwq.enableyContentId = q.enableyContentId

