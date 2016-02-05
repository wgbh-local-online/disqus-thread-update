class ::ThreadDatabase

  DATABASE = 'disqus'

  attr_reader :client

  def initialize(wipe = false)
    @client = Mysql2::Client.new(
      :host   => 'localhost',
      :username => "#{DATABASE}_user",
      :password => 'password123',
      :database => DATABASE
    )
    @tables = define_tables
    drop_tables if wipe
    install_tables
  end
  
  def drop_tables
    @tables.each do |table|
      sql = "DROP TABLE IF EXISTS #{table[:name]}"
      @client.query(sql)
    end
  end

  
  def needs_tables
    existing_tables = []
    @client.query("SHOW TABLES;").each do |row|
      existing_tables << row
    end
    existing_tables.count != @tables.count 
  end
  
  def install_tables  
    if needs_tables
      @tables.each do |table|
      
        # Create table
        sql = "CREATE TABLE IF NOT EXISTS #{table[:name]} ("
        table[:columns].each do |col_name, col_type|
          sql += "#{col_name} #{col_type},"
        end
        sql = sql.chomp(",") + ");"
        puts sql
        @client.query(sql)
        
        # Set primary key
        sql = "ALTER TABLE #{table[:name]} ADD PRIMARY KEY (#{table[:primary_key]});"
        @client.query(sql)
        
        # Set indexes
        unless table[:indexes].nil?
          table[:indexes].each do |column| 
            sql = "CREATE INDEX #{table[:name]}_#{column} ON #{table[:name]} (#{column});"
            puts sql
            @client.query(sql)
          end
        end
      end
    end
  end
  
  def define_tables
    [
      {
        :name => 'threads',
        :columns => {
          'thread_id' => 'varchar(30)',
          'original_identifier' => 'varchar(30)',
          'link' => 'varchar(255)',
          'new_identifier' => 'varchar(30)',
          'updated' =>  'tinyint',
          'posts' => 'integer'
        },
        :primary_key => 'thread_id',
        :indexes => ['posts', 'original_identifier']
      }, {
        :name => 'node_mapping',
        :columns => {
          'old_node' => 'varchar(30)',
          'new_node' => 'varchar(30)'
        },
        :primary_key => 'old_node'
      }
    ]
  end  
end 
