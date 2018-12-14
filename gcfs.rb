require "rfusefs"
require "gocardless_pro"
require "json"

GCCLIENT = GoCardlessPro::Client.new(
  access_token: ENV["GC_ACCESS_TOKEN"],
  environment: ENV["GC_ENVIRONMENT"].to_sym,
)

def to_filename(path)
  path.gsub(/^\//, '')
end

class ExpirableObjectCache
  def initialize(expire_after_seconds: 15)
    @cache = {}
    @expire_after_seconds = expire_after_seconds
  end

  def get(key, on_miss:)
    if (not @cache.include?(key)) || expired?(key)
      puts "cache miss, repopulate #{key}"
      @cache[key] = {
        expire_at: DateTime.now + Rational(@expire_after_seconds, 86400),
        value: on_miss.call()
      }
    end

    @cache[key][:value]
  end

  def expired?(key)
    expiration = @cache.fetch(key, {
      expire_at: DateTime.parse("1970-01-01 00:00")
    })[:expire_at]

    DateTime.now >= expiration
  end

  def invalidate(key)
    @cache.delete(key)
  end
end

module GCFS
  class ApiFS < FuseFS::FuseDir
    def initialize(api_root)
      @api_root = api_root
      @cache = ExpirableObjectCache.new(expire_after_seconds: 60)
    end

    def contents(path)
      list.map { |obj| obj.id } + ["_create"]
    end

    def file?(path)
      path == "/_create" || list.map { |obj| "/#{obj.id}" }.include?(path)
    end

    def directory?(path)
      false
    end

    def can_write?(path)
      path == "/_create"
    end

    def read_file(path)
      return "" if path == "/_create"
      @cache.get(path, on_miss: -> { @api_root.get(to_filename(path)).to_h.to_json })
    end

    def write_to(path, content)
      return if content.empty?
      params = JSON.parse(content)
      @api_root.create(params: params)
      @cache.invalidate(path)
      @cache.invalidate("_list")
    end

    private

    def list
      @cache.get("_list", on_miss: -> { @api_root.list.records })
    end
  end

  class RootFS < FuseFS::FuseDir
    PATH_MAP = {
      "customers" => ApiFS.new(GCCLIENT.customers),
      "payments" => ApiFS.new(GCCLIENT.payments),
      "mandates" => ApiFS.new(GCCLIENT.mandates),
      "payouts" => ApiFS.new(GCCLIENT.payouts),
      "events" => ApiFS.new(GCCLIENT.events),
    }

    def contents(path)
      return PATH_MAP.keys if path == "/"
      proxy_to(path, :contents)
    end

    def file?(path)
      return false if path == "/"
      return false unless valid?(path)
      proxy_to(path, :file?)
    end


    def directory?(path)
      return true if path == "/"
      return true if PATH_MAP.include?(path.gsub(/^\//, ''))
      return false unless valid?(path)
      proxy_to(path, :directory?)
    end

    def read_file(path)
      proxy_to(path, :read_file)
    end

    def write_to(path, content)
      puts "writing to #{path}"
      proxy_to(path, :write_to, str: content)
    end

    def can_write?(path)
      return false if PATH_MAP.include?(path.gsub(/^\//, ''))
      return false unless valid?(path)
      proxy_to(path, :can_write?)
    end

    private

    def subfs(component)
      PATH_MAP[component.to_s]
    end

    def valid?(path)
      PATH_MAP.include?(component(path).to_s)
    end

    def proxy_to(path, method, str: nil)
      root, rest = split_path(path)
      if str.nil?
        subfs(root).send(method, rest)
      else
        subfs(root).send(method, rest, str)
      end
    end

    def component(path)
      split_path(path).first
    end
  end
end


if (File.basename($0) == File.basename(__FILE__))
    root = GCFS::RootFS.new
    FuseFS.set_root(root)
    FuseFS.mount_under(ARGV[0])
    puts "Running, mounted on '#{ARGV[0]}'"
    FuseFS.run
end
