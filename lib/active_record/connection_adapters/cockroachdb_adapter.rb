require 'active_record/connection_adapters/postgresql_adapter'


module ActiveRecord
  module ConnectionHandling
    def cockroachdb_connection(config)
      # This is copied from the PostgreSQL adapter.
      conn_params = config.symbolize_keys

      conn_params.delete_if { |_, v| v.nil? }

      # Map ActiveRecords param names to PGs.
      conn_params[:user] = conn_params.delete(:username) if conn_params[:username]
      conn_params[:dbname] = conn_params.delete(:database) if conn_params[:database]

      # Forward only valid config params to PGconn.connect.
      valid_conn_param_keys = PGconn.conndefaults_hash.keys + [:requiressl]
      conn_params.slice!(*valid_conn_param_keys)

      # The postgres drivers don't allow the creation of an unconnected PGconn object,
      # so just pass a nil connection object for the time being.
      ConnectionAdapters::CockroachDBAdapter.new(nil, logger, conn_params, config)
    end
  end
end

class ActiveRecord::ConnectionAdapters::CockroachDBAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
  ADAPTER_NAME = "CockroachDB".freeze
  def indexes(table_name, name = nil) # :nodoc:
    # The PostgreSQL adapter uses a correlated subquery in the following query,
    # which CockroachDB does not yet support. The query is modified to use a
    # GROUP BY and CROSS JOIN instead.
    if name
      ActiveSupport::Deprecation.warn(<<-MSG.squish)
        Passing name to #indexes is deprecated without replacement.
      MSG
    end

    table = Utils.extract_schema_qualified_name(table_name.to_s)

    result = query(<<-SQL, "SCHEMA")
      SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid,
                      pg_catalog.obj_description(i.oid, 'pg_class') AS comment,
                      count(opcdefault) AS opclass
      FROM pg_class t
      INNER JOIN pg_index d ON t.oid = d.indrelid
      INNER JOIN pg_class i ON d.indexrelid = i.oid
      LEFT JOIN pg_namespace n ON n.oid = i.relnamespace
      CROSS JOIN unnest(d.indclass) classoid
      LEFT JOIN pg_opclass ON classoid = pg_opclass.oid
      AND pg_opclass.opcdefault = 'f'
      WHERE i.relkind = 'i'
        AND d.indisprimary = 'f'
        AND t.relname = '#{table.identifier}'
        AND n.nspname = #{table.schema ? "'#{table.schema}'" : 'ANY (current_schemas(false))'}
      GROUP BY i.relname, indisunique, indkey, pg_get_indexdef, t.oid, comment
      ORDER BY i.relname
    SQL

    result.map do |row|
      index_name = row[0]
      unique = row[1]
      indkey = row[2].split(" ").map(&:to_i)
      inddef = row[3]
      oid = row[4]
      comment = row[5]
      opclass = row[6]

      using, expressions, where = inddef.scan(/ USING (\w+?) \((.+?)\)(?: WHERE (.+))?\z/).flatten

      if indkey.include?(0) || opclass > 0
        columns = expressions
      else
        columns = Hash[query(<<-SQL.strip_heredoc, "SCHEMA")].values_at(*indkey).compact
          SELECT a.attnum, a.attname
          FROM pg_attribute a
          WHERE a.attrelid = #{oid}
          AND a.attnum IN (#{indkey.join(",")})
        SQL

        # add info on sort order for columns (only desc order is explicitly specified, asc is the default)
        orders = Hash[
          expressions.scan(/(\w+) DESC/).flatten.map { |order_column| [order_column, :desc] }
        ]
      end

      IndexDefinition.new(table_name, index_name, unique, columns, [], orders, where, nil, using.to_sym, comment.presence)
    end.compact
  end
end