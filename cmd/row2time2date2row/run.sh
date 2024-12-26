#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

genpgcopy(){
	echo "

		COPY (
			SELECT
				'fuji'::TEXT AS name,
				'{}'::BYTEA AS data,
				404::INTEGER AS status,
				42::BIGINT AS id,
				100.0::REAL AS ratio,
				3.776::DOUBLE PRECISION AS height,
				FALSE::BOOLEAN AS active,
				'cafef00d-dead-beaf-face-864299792458'::UUID AS related,
				CLOCK_TIMESTAMP()::TIMESTAMP WITH TIME ZONE AS created,
				NULL::BIGINT AS updated,
				CLOCK_TIMESTAMP()::TIMESTAMP WITH TIME ZONE AS processed
	
			UNION ALL
	
			SELECT
				'takao'::TEXT AS name,
				'{}'::BYTEA AS data,
				403::INTEGER AS status,
				43::BIGINT AS id,
				99.1::REAL AS ratio,
				0.599::DOUBLE PRECISION AS height,
				TRUE::BOOLEAN AS active,
				'cafef00d-dead-beaf-face-864299792458'::UUID AS related,
				CLOCK_TIMESTAMP()::TIMESTAMP WITH TIME ZONE AS created,
				(EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())*1e6)::BIGINT AS updated,
				NULL::TIMESTAMP WITH TIME ZONE AS processed
	
		)
		TO STDOUT
		WITH (
			FORMAT BINARY
		)

	" |
	env PGUSER=postgres LC_ALL=C psql |
	cat > ./sample.d/sample.pgcopy
}

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.pgcopy |
		pgcopy2avro |
		cat > ./sample.d/sample.avro
}

#genpgcopy
#genavro

export ENV_TIME_COLNAME=created
export ENV_DATE_COLNAME=created_date
export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

cat sample.d/sample.avro |
	./row2time2date2row |
	rq -aJ |
	jq -c
