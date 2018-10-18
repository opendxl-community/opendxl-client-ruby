Gem::Specification.new do |s|
  s.name = 'dxlclient'
  s.version = '0.0.1'
  s.author = 'Jeremy Barlow'
  s.licenses = ['Apache-2.0']
  s.summary = 'OpenDXL Ruby client'
  s.files = Dir['lib/**/*.rb']

  s.add_runtime_dependency 'iniparse', '~> 1.4.4'
  s.add_runtime_dependency 'mqtt', '0.5.0'
  s.add_runtime_dependency 'msgpack', '~> 1.2.2'

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rubocop'
  s.add_development_dependency 'yard'
end
