require 'optparse'
require 'optparse/time'
require 'ostruct'
require 'pp'

class ::RuntimeOptions

  ACTIONS = [ 'load_node_map', 'fetch_data_from_disqus', 'setup_database' ]
  ACTION_ALIASES = {
    'map' => 'load_node_map',
    'fetch' => 'fetch_data_from_disqus',
    'setup' => 'setup_database'
  }

  def self.parse(args)
  
    options = OpenStruct.new
    
    # Set option defaults
    options.wipe = false
    options.limit = 10
    options.full = false
    options.mapping_file = 'Node-ID-Mappings.csv'
    
    opt_parser = OptionParser.new do |opts|
    
      opts.banner = "Usage: disqus_update_thread.rb [options]"

      opts.separator ""
      opts.separator "Options: "
      
      # Require an action
      action_list = (ACTION_ALIASES.map {|k,v| "#{v} (#{k})"}).join(',')
      opts.on("-a", "--action ACTION", ACTIONS, ACTION_ALIASES, 
        "You must specify an action (#{action_list})") do |action|
          options.action = action
      end  
      
      # Optional arguments
      opts.on("-w", "--wipe", "Drop database tables and recreate") do |wipe|
        options.wipe = wipe
      end
      opts.on("-f", "--full", "Full thread dataset") do |full|
        options.full = full
      end
      opts.on("-l", "--limit [LIMIT]", "Number of threads to process (default: #{options.limit})") do |limit|
        options.limit = limit
      end
      opts.on("-m", "--mapping-file [MAPPING FILE]", "Path to node map file (default: #{options.mapping_file})") do |mapping_file|
        options.mapping_file = mapping_file
      end
      
      opts.on("-q", "--queries [NUMBER]", "Number of queries to make to the API if not doing a full run") do |queries|
        options.queries = queries
      end
  
      opts.on_tail("-h", "--help", "Show this message") do
        puts opts
        exit
      end
    end
    
    opt_parser.parse!(args)
    
    # Require an action
    if options.action.nil?
      print "Please specify an action: \n"
      ACTIONS.each_with_index do |a,i|
        puts "(#{i+1}) #{a}" 
      end
      options.action = ACTIONS[(gets.chomp.to_i) - 1]
    end
    
    # Check to see if map file exists if it is going to be read
    if options.action == 'load_node_map'
      unless File.file? options.mapping_file
        puts "The map file cannot be found.\n"
        exit
      end
    end
    
    # Handle full option
    if options.full
      unless options.action == 'fetch_data_from_disqus'
        puts "The -f (--full) option has no effect for this action\n"
      else
        puts "Setting the limit option to 100 (Disqus maximum) \n"
        options.limit = 100
      end
    end
    
    # Double check on wiping the database
    if options.wipe
      puts "You will be wiping the database. Are you sure? (Y) "
      exit unless gets.chomp! == 'Y'
    end
    options
  end
end

