WITH CTE AS (
	SELECT 
        CAST(ca.enableyChannelId AS TEXT) AS [EnableyChannelId],
        CAST(
            REPLACE(
                REPLACE(ca.channelCategories, 'CRIAR: ', ''),
                    ';', '; ') AS TEXT) AS [Categoria da Trilha],
		CAST(cv.channelName AS TEXT) AS [Nome da Trilha],
		CAST(cv.channelId AS INT) AS [Id da Trilha],
		-- replace utilizado para fazer correções de expressões de html.
         CAST(   
            REPLACE(    
                REPLACE(    
                    REPLACE( 
                        REPLACE(
                            REPLACE(
                                    REPLACE(
                                        REPLACE(cv.description, '<p>', ''),
                                            '</p>', ''),
                                                '&gt;', '>'),
                                                    '&nbsp;', ' '),
                                                        '<div>', ''),
                                                            '</div>', ''),
                                                                '<br>', '') AS TEXT)
                                        AS [Descrição do Curso],
		CAST(cv.durationStr AS TEXT) AS [Carga-Horária],
		CAST(cv.creatorUserId AS INT) AS [Id do Criador],
		CAST(CONCAT(ubCreator.firstName, ' ', ubCreator.lastName) AS TEXT) AS [Nome do Criador],
		CASE
			WHEN 
				ISDATE(LEFT(cv.creationDate, 10)) = 1
			THEN 
				CONVERT(DATE, LEFT(cv.creationDate, 10), 120)
			ELSE NULL
		END AS [Data de Criação],
		CASE
			WHEN 
				ISDATE(LEFT(ci.lastVersionDate, 10)) = 1
			THEN 
				CONVERT(DATE, LEFT(ci.lastVersionDate, 10), 120)
			ELSE NULL
		END AS [Data de Última Edição],
		CAST(ci.publisherUsername AS INT) AS [Id Último Editor],
		CAST(CONCAT(ubEditor.firstName, ' ', ubEditor.lastName) AS TEXT) AS [Nome do Último Editor],
		ROW_NUMBER() OVER (PARTITION BY ca.enableyChannelId ORDER BY ca.contentId) AS rn
	FROM 
		course_assigned ca
	LEFT JOIN 
		academiavivo_channels_view cv
	ON 
		ca.enableyChannelId = cv.enableyChannelId
	LEFT JOIN
		academiavivo_content_items ci
	ON 
		ca.enableyChannelId = ci.originalEnableyChannelId
	LEFT JOIN
		users_base ubCreator
	ON
		cv.creatorUserId = ubCreator.externalId
	LEFT JOIN 
		users_base ubEditor
	ON
		ci.publisherUsername = ubEditor.externalId
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
)

SELECT 
    [EnableyChannelId],
    [Id da Trilha],
    [Data de Criação],
    [Data de Última Edição],
    [Categoria da Trilha],
    [Nome da Trilha],
    [Descrição do Curso],
    [Carga-Horária],
    [Id do Criador],
    [Nome do Criador],
    [Id Último Editor],
    [Nome do Último Editor]
FROM 
	CTE
WHERE
	rn = 1
ORDER BY
	[Data de Criação] DESC;