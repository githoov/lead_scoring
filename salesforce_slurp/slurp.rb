# rvm use jruby-1.7.19
# bundle exec ruby slurp.rb
# TODO: push notification from copystorm machine when:
# tail | grep -i 'copy is complete' copystorm.log

# preliminaries #
require 'restforce'
require 'looker-sdk'
require 'json'
require 'sequel'

# LOOKER PORTION #

# instantiate the Looker client #
looker = LookerSDK::Client.new(
  :client_id => "",
  :client_secret => "",
  :api_endpoint => ""
)

max_time = looker.run_inline_query("json", {:model => "sales_development", :view => "lead", :fields => ["lead.created_time"], :sorts => ["lead.created_time desc"], :limit => 1})

max_time.first["lead.created_time"]

# get leads to score #
leads = looker.run_look(2181, 'json')

# remove 'lead_scoring.' from hash keys #
leads_new = leads.map {|h| h.inject({}){|new, (k, v)| new[k.to_s.gsub("lead_scoring.", "").to_sym] = v; new}}

# auto-incremented integer for batch numbering #
auto_int = File.read("auto_int.txt").to_i + 1

# overwrite previous batch number #
File.open("auto_int.txt", "w") do |file|
  file.write(auto_int)
end

# create an auto-incremented batch number #
batch_number = "batch-#{auto_int}"

# JPMML PORTION #

# structure json of leads for request #
request_json = {"id" => batch_number, "requests" => leads_new.map{|h| {"id" => h[:lead_id], "arguments" => h.select {|k,v| k.to_s != "lead_id"}}}}.to_json

# construct request #
uri = URI.parse("http://foobar.herokuapp.com/openscoring/model/BayesLeadScore/batch")
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.path, initheader = {'Content-Type' =>'application/json'})
request.body = "#{request_json}"

# capture response #
response = http.request(request)
puts "Response #{response.code} #{response.message}: #{response.body}"

# loop through response and update Salesforce Lead record with meeting probability #
to_put = JSON.parse(response.body)["responses"].map{|k,v| k["id"]}.zip JSON.parse(response.body)["responses"].map{|h| h["result"]["Probability_1"]}


# SEQUEL PORTION #

DB = Sequel.connect('jdbc:mysql://database_host/database_name?user=user_name&password=user_password')

Sequel::Model.plugin :timestamps, update_on_create: true

DB.create_table?(:LeadScore) do
	primary_key :Id
	String :LeadId, :size => 20
	Float :Score
	DateTime :CreatedAt
	index :LeadId
	index :CreatedAt
end

class LeadScore < Sequel::Model(:LeadScore)
	plugin :timestamps
end

to_put.each do |id, probability|
	LeadScore.insert(:LeadId => id, :Score => probability)
end