module ActiveRecord
  module ConnectionAdapters
    module CockroachDB
      module SchemaStatements
        include ActiveRecord::ConnectionAdapters::PostgreSQL::SchemaStatements

        DEFAULT_PRIMARY_KEY = "rowid"

        # copied from ActiveRecord::PostgreSQL::SchemaStatements
        #
        # - removed the comment part from the CREATE INDEX statement
        def add_index(table_name, column_name, options = {})
          index_name, index_type, index_columns_and_opclasses, index_options, index_algorithm, index_using, _comment = add_index_options(table_name, column_name, options)
          execute("CREATE #{index_type} INDEX #{index_algorithm} #{quote_column_name(index_name)} ON #{quote_table_name(table_name)} #{index_using} (#{index_columns_and_opclasses})#{index_options}")
        rescue ActiveRecord::StatementInvalid => error
          if debugging? && error.cause.class == PG::FeatureNotSupported
            warn "#{error}\n\nThis error will be ignored and the index will not be created.\n\n"
          else
            raise error
          end
        end

        # ActiveRecord allows for tables to exist without primary keys.
        # Databases like PostgreSQL support this behavior, but CockroachDB does
        # not. If a table is created without a primary key, CockroachDB will add
        # a rowid column to serve as its primary key. This breaks a lot of
        # ActiveRecord's assumptions so we'll treat tables with rowid primary
        # keys as if they didn't have primary keys at all.
        # https://www.cockroachlabs.com/docs/v19.2/create-table.html#create-a-table
        # https://api.rubyonrails.org/v5.2.4/classes/ActiveRecord/ConnectionAdapters/SchemaStatements.html#method-i-create_table
        def primary_key(table_name)
          pk = super

          if pk == DEFAULT_PRIMARY_KEY
            nil
          else
            pk
          end
        end

        # copied from ActiveRecord::PostgreSQL::SchemaStatements
        #
        # - removed the algortithm part from the DROP INDEX statement
        # - added CASCADE because cockroach won't drop a UNIQUE constrain without
        def remove_index(table_name, options = {})
          table = PostgreSQL::Utils.extract_schema_qualified_name(table_name.to_s)

          if options.is_a?(Hash) && options.key?(:name)
            provided_index = PostgreSQL::Utils.extract_schema_qualified_name(options[:name].to_s)

            options[:name] = provided_index.identifier
            table = PostgreSQL::Name.new(provided_index.schema, table.identifier) unless table.schema.present?

            if provided_index.schema.present? && table.schema != provided_index.schema
              raise ArgumentError, "Index schema '#{provided_index.schema}' does not match table schema '#{table.schema}'"
            end
          end

          index_to_remove = PostgreSQL::Name.new(table.schema, index_name_for_remove(table.to_s, options))
          execute "DROP INDEX #{quote_table_name(index_to_remove)} CASCADE"
        end
      end
    end
  end
end
