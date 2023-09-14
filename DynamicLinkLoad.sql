CREATE PROC DynamicLinkLoad (@link VARCHAR(128))
AS
BEGIN
	-- GET SOURCE TABLE ---
	DECLARE @source_table VARCHAR(128)

	SET @source_table = ''
	SET @source_table = (
			SELECT TOP (1) StageSource
			FROM ColMappingLinks
			WHERE LinkDest = @link
			)

	--- GET ALL LINK COLUMNS ---
	DECLARE @link_columns VARCHAR(max)

	SET @link_columns = ''
	SET @link_columns = (
			SELECT string_agg(COLUMN_NAME, ', ') AS col_list
			FROM information_schema.columns
			WHERE table_name = @link
			)

	-- GET COLS THAT ARE IN STAGE TABLE-- 
	DECLARE @stage_columns VARCHAR(max)

	SET @stage_columns = ''
	SET @stage_columns = (
			SELECT string_agg(StageCol, ', ') AS col_list
			FROM ColMappingLinks
			WHERE LinkDest = @link
				AND isPK = 1
			)

	-- GET PRIMARY KEYS from stage --- 
	DECLARE @primary_keys VARCHAR(max)

	SET @primary_keys = ''
	SET @primary_keys = (
			SELECT string_agg(StageCol, ',''|'', ') AS col_list
			FROM ColMappingLinks
			WHERE LinkDest = @link
				AND isPK = 1
			)

	--print @primary_keys
	-- GET LINKKEY--
	DECLARE @LinkKey VARCHAR(128)

	SELECT @LinkKey = QUOTENAME(name)
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@link)
		AND sys.columns.name LIKE 'Link%Key';

	----- HUB KEYS FOR HUB..KEY ----
	DECLARE @HubList VARCHAR(max)

	SET @HubList = (
			SELECT string_agg(StageCol, ', ') AS col_list
			FROM ColMappingLinks
			WHERE LinkDest = @link
				AND LinkCol LIKE 'Hub%'
			)

	--- create HubXKey rows ---
	DECLARE @HubRows VARCHAR(max)

	SET @HubRows = ''

	DECLARE @HubKey VARCHAR(max)

	WHILE len(@HubList) > 0
	BEGIN
		IF CHARINDEX(',', @HubList) > 0
		BEGIN
			SET @HubKey = SUBSTRING(@HubList, 1, CHARINDEX(',', @HubList) - 1)
			SET @HubList = REPLACE(@HubList, @HubKey + ', ', '')
		END
		ELSE IF CHARINDEX(',', @HubList) = 0
		BEGIN
			SET @HubKey = @HubList
			SET @HubList = LEFT(@HubList, LEN(@HubList) - LEN(@HubList))
		END

		IF @HubKey LIKE '%date%'
			SET @HubRows = @HubRows + 'CONVERT(VARCHAR(128),  HASHBYTES(''MD5'', CONVERT(VARCHAR(20),' + @HubKey + ',112)), 2) AS Hub' + @HubKey + 'Key ,'
		ELSE
			SET @HubRows = @HubRows + 'CONVERT(VARCHAR(128), HASHBYTES(''MD5'', ' + @HubKey + '), 2) AS Hub' + @HubKey + 'Key ,'
	END

	--- LOAD LINK --- 
	DECLARE @sql1 NVARCHAR(max)

	SET @sql1 = N'
		
			SELECT 
				   CONVERT(VARCHAR(128), HASHBYTES(''MD5'', CONCAT (' + @primary_keys + ')), 2) AS ' + @LinkKey + ',' + @HubRows + '
				   CONVERT(VARCHAR(128), HASHBYTES(''MD5'', ''stage.' + @source_table + '''), 2) AS [LinkRecordSourceId],
				   GETDATE() AS [LinkLoadDate],
				   ' + @stage_columns + ' 
			INTO #temp
			FROM stage.' + @source_table + ' 
		

			insert into ' + @link + ' (' + @link_columns + ')
			select *
			from #temp tmp
			WHERE  NOT EXISTS (SELECT 1
							FROM ' + @link + ' link
							WHERE link.' + @LinkKey + ' = tmp.' + @LinkKey + ' 
							)
			
			  
			drop table #temp'

	EXEC sp_executesql @sql1;
END

EXEC DynamicLinkLoad 'LINK_EMPLOYEE_MANAGER'


SELECT * FROM LINK_EMPLOYEE_MANAGER
DROP PROC DynamicLinkLoad
