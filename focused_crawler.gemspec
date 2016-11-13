# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'focused_crawler/version'

Gem::Specification.new do |spec|
  spec.name          = 'focused_crawler'
  spec.version       = FocusedCrawler::VERSION
  spec.authors       = ['yuta-muramoto']
  spec.email         = ['s1513114@u.tsukuba.ac.jp']

  spec.summary       = 'It is a focused crawler.'
  spec.description   = 'It is a focused crawler that used distributed technology.'
  spec.homepage      = 'https://github.com/yuta-muramoto/focused_crawler'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
                          f.match(%r{^(test|spec|features)/})
                        end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'nokogiri'

  spec.add_development_dependency 'bundler', '~> 1.12'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'minitest', '~> 5.0'
  spec.add_development_dependency 'minitest-power_assert'
  spec.add_development_dependency 'minitest-doc_reporter'
  spec.add_development_dependency 'minitest-stub_any_instance'
  spec.add_development_dependency 'pry'
end
