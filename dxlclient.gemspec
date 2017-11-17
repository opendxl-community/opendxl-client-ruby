Gem::Specification.new do |s|
  s.name = 'dxlclient'
  s.version = '0.0.1'
  s.author = 'Jeremy Barlow'
  s.licenses = ['Apache-2.0']
  s.summary = 'OpenDXL Ruby client'
  s.files = Dir['lib/**/*.rb']

  s.add_runtime_dependency 'iniparse'
  s.add_runtime_dependency 'mqtt'
  s.add_runtime_dependency 'msgpack'

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rubocop'
  s.add_development_dependency 'yard'
end
