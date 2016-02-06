#!/usr/local/bin/ruby
require 'rubygems'
require 'bundler/setup'

require 'disqus_api'
require 'json'
require 'csv'
require 'mysql2'
require 'logger'
require 'optparse'
require 'pry'
require './thread_database'
require './runtime_options'

options = RuntimeOptions.parse(ARGV)

# Information array
$info = {
  :threads => {
    :processed => 0,
    :skipped => 0,
    :failed => 0
  },
  :queries => 0
}


# Create logger
if $stdout.isatty
  logfile = STDOUT
else
  logfile = "disqus-connection-#{Time.now.strftime('%Y%m%d_%H%M')}.log"
end

$logger = Logger.new(logfile)
$logger.info "Running with options: #{options}"

# Open database
$db = ThreadDatabase.new(options)

# Method defs
def save_threads(threads)
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
      sql = "INSERT INTO threads (#{thread_data.keys.join(',')}) VALUES (#{thread_data.values.map{ |v| "'#{v}'" }.join(',')});"
  #                    ^-- Ignore duplicates (mysql proprietary)
      begin
        $db.client.query(sql)
        $info[:threads][:processed] += 1
      rescue Mysql2::Error => e
        match = /Duplicate entry '(\d+)' for key 'PRIMARY'/.match(e.message)
        if match
          $logger.info "Skipping thread #{match[1]}"
          $info[:threads][:skipped] += 1
        else
          $logger.debug e.message
          $info[:threads][:failed] += 1
        end
      end
    end
  end
end

# This is where the bulk of the work gets done.
def fetch_data_from_disqus(options)
  
  DisqusApi.config = {
  # News migration
    api_secret: 'z3lCrBYm5mG5iIZnFsLJKmeyjNKuqiRXdWCteLNIYX5Q1kSriwdd7o74KbIuwswq',
    api_key: '42Z82KltaCNxz8rzTBPCWARfHzqzNO74BclcGX6rbQ76U6m6rMJnKt2ndusbBc2i',
    access_token: '2f7fa7d65f204b33a1d86b56b855dcff'
  }

  cursor_hash = { :hasNext => true }
  reset_time = 0
  processed = 0
  failed = 0
  
  if options.full || !options.queries.nil?    # Do a while loop if doing multiple iterations
    while cursor_hash[:hasNext] && (options.full || ($info[:queries] < options.queries.to_i) )
      cursor_hash = make_query(cursor_hash, options.limit)
      $info[:queries] += 1
      sleep(10)                               # Wait a decent time so the API doesn't get overloaded
    end
  else                                         # Otherwise just do the query once
    make_query(cursor_hash, limit)
  end
end

def make_query(cursor_hash, limit)
  begin
    threads = DisqusApi.v3.forums.listThreads(forum: 'wgbhnews', order: 'asc', limit: limit, cursor: cursor_hash[:next] )
    cursor_hash = threads[:cursor]
    reset_time = threads[:ratelimit_headers].nil? ? reset_time : threads[:ratelimit_headers]['x-ratelimit-reset']
    save_threads(threads[:response])
  rescue DisqusApi::InvalidApiRequestError => e
    # Abort unless it's a rate limit problem
    unless (e.response['code'] == 13)
      $logger.fatal "Program aborted with error: #{e.response.inspect}"
      $logger.info $info
      abort('Aborted with error.')  
    end
    
    # Wait until the reset time to start querying again
    wait_time = [(reset_time - Time.now.to_i) + 10, 0].max
    $logger.info "Waiting #{wait_time/60} minutes until #{Time.at(reset_time).to_datetime}"
    sleep(wait_time)
  end
  cursor_hash
end

def load_node_map(options)
  CSV.foreach(options.mapping_file) do |line|
    if /^\s*\d+\s*$/.match(line[0]) && /^\s*\d+\s*$/.match(line[0])
      sql = "INSERT IGNORE INTO node_mapping (old_node, new_node) VALUES ('#{line[0]}', '#{line[1]}');"
      $db.client.query(sql)
    end
  end    
end 


#============================================================


# Execute
unless options.action == 'setup_database'
  send(options.action, options)
else
  logger.info 'Database tables have been set up but not loaded.'
end

$logger.info $info



