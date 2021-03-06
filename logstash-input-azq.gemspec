Gem::Specification.new do |s|
  s.name          = 'logstash-input-azq'
  s.version       = '0.1.0'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Write a short summary, because Rubygems requires one.'
  s.description   = 'Write a longer description or delete this line.'
  s.homepage      = 'https://www.google.com'
  s.authors       = ['mxrss']
  s.email         = ''
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }


  # Gem dependencies
  #s.add_runtime_dependency "azure-storage-queue",
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'stud', '>= 0.0.22'
  s.add_runtime_dependency 'azure-storage-queue', "~> 1.0"
  s.add_development_dependency 'logstash-devutils', '>= 0.0.16'
end
