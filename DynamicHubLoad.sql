CREATE PROC DynamicHubLoad (@hub VARCHAR(128))
AS
BEGIN
	IF @hub != 'HUB_DATES'
	BEGIN
		--GET SOURCE TABLE---
		DECLARE @source_table VARCHAR(128)

		SET @source_table = (
				SELECT Stage
				FROM MapStageToHub
				WHERE Hub = @hub
				)

		PRINT @source_table

		--- GET HUB COLUMNS ---
		DECLARE @hub_columns VARCHAR(max)

		SET @hub_columns = (
				SELECT string_agg(COLUMN_NAME, ', ') AS col_list
				FROM information_schema.columns
				WHERE table_name = @hub
				)

		PRINT @hub_columns

		--- GET PRIMARY KEYS --- 
		DECLARE @primary_keys VARCHAR(max)

		SET @primary_keys = (
				SELECT C.COLUMN_NAME
				FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
				JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C
					ON C.CONSTRAINT_NAME = T.CONSTRAINT_NAME
				WHERE C.TABLE_NAME = @source_table
					AND T.CONSTRAINT_TYPE = 'PRIMARY KEY'
				)

		PRINT @primary_keys

		-- hub primary key---
		DECLARE @HUB_primary_keys VARCHAR(max)

		SET @HUB_primary_keys = (
				SELECT HubPK
				FROM MapHubtoHubPK
				WHERE Hub = @hub
				)

		PRINT @HUB_primary_keys

		--- LOAD HUB---
		DECLARE @sql NVARCHAR(max)

		SET @sql = 'INSERT INTO ' + @hub + '(' + @hub_columns + ')

		 SELECT CONVERT(VARCHAR(128),HASHBYTES(''SHA'',' + @primary_keys + '),2),
			   CONVERT(VARCHAR(128),HASHBYTES(''MD5'', ''stage.' + @source_table + '''),2),
			   GETDATE(),' + @primary_keys + '
		FROM stage.' + @source_table + '
		WHERE NOT EXISTS 
		(
			SELECT 1
			FROM ' + @hub + ' WHERE 
			' + @hub + '.' + @HUB_primary_keys + ' = ' + @source_table + '.' + @primary_keys + '
		)';

		EXEC sp_executesql @sql;
	END
	ELSE
	BEGIN
		DECLARE @sql_spec NVARCHAR(max)

		SET @sql_spec = N'INSERT INTO dbo.HUB_DATES (HubDateKey, HubRecordSourceId, HubLoadDate, Date)
			SELECT CONVERT(VARCHAR(128),HASHBYTES(''MD5'', CONVERT(VARCHAR(20),create_date,112)),2),
				   CONVERT(VARCHAR(128),HASHBYTES(''MD5'', ''stage.master_orders, stage.sales_pipeline''),2),
				   GETDATE(),
				   create_date 
			FROM ( 
					SELECT create_date FROM stage.master_orders 
					UNION
					SELECT close_date FROM stage.sales_pipeline
					where close_date is not NULL 
				 ) as selected_dates
			WHERE NOT EXISTS 
			(
				SELECT 1 
				FROM dbo.HUB_DATES Hub
				WHERE Hub.Date = create_date
			)';

		EXEC sp_executesql @sql_spec;
	END
END;

EXEC DynamicHubLoad 'HUB_EMPLOYEES'
