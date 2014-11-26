=begin
  Usage: bundle exec build_algolia_index.rb CITIES_FILENAME

  - Cities data: download and unzip file from http://download.geonames.org/export/dump/cities1000.zip
  - algolia crendentials must be available in environment variables ALGOLIA_APPLICATION_ID and ALGOLIA_API_KEY
=end

require 'csv'
require 'countries'
require 'algoliasearch'

if ARGV.size == 0
  puts 'ERROR: missing CITIES_FILENAME argument'
  exit 1
end
CITIES_FILE = ARGV[0]


def load_cities_from_geoname_file(cities_filename)
  puts 'Loading data from file...'
  cities = []
  File.open(cities_filename, 'r') do |f|
    f.each_line do |line|
      begin
        row = CSV.parse(line, :col_sep => "\t")[0]
        country = Country.find_country_by_alpha2(row[8])
        city = {
            :name       => row[1],
            :country    => country ? country.name : '',
            :population => row[14].to_i,
            :_geoloc    => {
                              :lat => row[4],
                              :lng => row[5]
                            }
        }
        cities << city
      rescue CSV::MalformedCSVError
        # ignore malformed lines
      end
    end
  end
  cities
end


# -- reset Algolia index

Algolia.init :application_id => ENV['ALGOLIA_APPLICATION_ID'],
             :api_key        => ENV['ALGOLIA_API_KEY']


index = Algolia::Index.new('OpenStreetMap')
index.clear_index
index.set_settings({
                       :attributesToIndex => ['name', 'country'],
                       :customRanking => ['desc(population)']
                   })


# -- upload data to Algolia

cities = load_cities_from_geoname_file(CITIES_FILE)
batch_size = 1000
nb_slices = cities.size / batch_size
cities.each_slice(batch_size).with_index do |batch, i|
  puts "Uploading slice #{i}/#{nb_slices}..."
  index.add_objects(batch)
end
