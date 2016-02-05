#!/Users/tim_kinnel/.rvm/rubies/ruby-2.2.1/bin/ruby
require 'rubygems'
require 'bundler/setup'

require 'disqus_api'
require 'json'
require 'csv'
require 'mysql2'
require 'logger'
require 'pry'
require './thread_database'

# Settings
BASE_DIRECTORY = "/Users/tim_kinnel/Projects/WGBH News/Disqus migration"
MAPPING_FILE = "#{BASE_DIRECTORY}/Node-ID-Mappings.csv"

# SWITCHES
WIPE = false
ACTION = 'load_node_map' # [ load_node_map fetch_data ]
ARGS = {}

# Create logger
logfile = "disqus-connection-#{Time.now.strftime('%Y%m%d_%H%M')}.log"
$logger = Logger.new(logfile)
$logger.info "Processing action: #{ACTION}"

# Open database
$db = ThreadDatabase.new(WIPE)

# Method defs
def save_threads(threads)
  status = {
    :processed => 0,
    :failed => 0
  }
  threads.each do |thread|
    thread_data = {
      'thread_id'           => thread['id'],
      'original_identifier' => thread['identifiers'][0],
      'link'                => thread['link'],
      'new_identifier'      => nil,
      'updated'             => 0,
      'posts'               => thread['posts']
    }
    
    # Insert only nodes, not users
    if /node/.match(thread_data['original_identifier'])
      sql = "INSERT IGNORE INTO threads (#{thread_data.keys.join(',')}) VALUES (#{thread_data.values.map{ |v| "'#{v}'" }.join(',')});"
  #                    ^-- Ignore duplicates (mysql proprietary)
      unless $db.client.query(sql)
        $logger.info "Failed: #{sql}"
        status[:processed] += 1
      else
        status[:failed] += 1
      end
    end
  end
  status
end

# This is where the bulk of the work gets done.
def fetch_data(args = {})

  options = {
    :limit => 10,
  }

  options.merge!(args)
  
  DisqusApi.config = {
  # News migration
    api_secret: 'z3lCrBYm5mG5iIZnFsLJKmeyjNKuqiRXdWCteLNIYX5Q1kSriwdd7o74KbIuwswq',
    api_key: '42Z82KltaCNxz8rzTBPCWARfHzqzNO74BclcGX6rbQ76U6m6rMJnKt2ndusbBc2i',
    access_token: '2f7fa7d65f204b33a1d86b56b855dcff'
  }

  cursor = { :next => nil }
  reset_time = 0
  processed = 0
  failed = 0
  
  # while cursor[:hasNext]
    begin
      threads = DisqusApi.v3.forums.listThreads(forum: 'wgbhnews', order: 'asc', limit: options[:limit], cursor: cursor[:next] )
      cursor = threads[:cursor]
      reset_time = threads[:ratelimit_headers].nil? ? reset_time : threads[:ratelimit_headers]['x-ratelimit-reset']
      status = save_threads(threads[:response])
      processed += status[:processed]
      failed    += status[:failed]
      $logger.info "Processed: #{processed}    | Failed: #{failed}"
    rescue DisqusApi::InvalidApiRequestError => e
      # Abort unless it's a rate limit problem
      unless (e.response['code'] == 13)
        $logger.fatal "Program aborted with error: #{e.response.inspect}"
        abort('Aborted with error.')  
      end
      
      # Wait until the reset time to start querying again
      wait_time = [(reset_time - Time.now.to_i) + 10, 0].max
      $logger.info "Waiting #{wait_time/60} minutes until #{Time.at(reset_time).to_datetime}"
      sleep(wait_time)
    end
  # end
end

def load_node_map(*args)
  CSV.foreach(MAPPING_FILE) do |line|
    if /^\s*\d+\s*$/.match(line[0]) && /^\s*\d+\s*$/.match(line[0])
      sql = "INSERT IGNORE INTO node_mapping (old_node, new_node) VALUES ('#{line[0]}', '#{line[1]}');"
      $db.client.query(sql)
    end
  end    
end 

#============================================================


# Execute
send(ACTION, ARGS)



