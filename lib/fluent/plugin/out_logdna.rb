# frozen_string_literal: true

require "fluent/output"

module Fluent
  class LogDNAOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output("logdna", self)

    MAX_RETRIES = 5

    config_param :api_key, :string, secret: true
    config_param :hostname, :string, default: :aptible
    config_param :ingester_domain, :string, default: "https://logs.logdna.com"
    config_param :ingester_endpoint, :string, default: "/logs/ingest"
    config_param :request_timeout, :string, default: "30"
    config_param :tags, :string, default: nil

    def configure(conf)
      super
      @host = conf["hostname"]
      @tags = conf["tags"]

      @tags_hash = {}
      @tags_hash = URI.decode_www_form(@tags).to_h.transform_keys(&:to_sym) if @tags

      # make these two variables globals
      timeout_unit_map = { s: 1.0, ms: 0.001 }
      timeout_regex = Regexp.new("^([0-9]+)\s*(#{timeout_unit_map.keys.join('|')})$")

      # this section goes into this part of the code
      num_component = 30.0
      unit_component = "s"

      timeout_regex.match(@request_timeout) do |match|
        num_component = match[1].to_f
        unit_component = match[2]
      end

      @request_timeout = num_component * timeout_unit_map[unit_component.to_sym]
    end

    def start
      super
      require "json"
      require "base64"
      require "http"
      HTTP.default_options = { keep_alive_timeout: 60 }
      @ingester = HTTP.persistent @ingester_domain
      @requests = Queue.new
    end

    def shutdown
      super
      @ingester.close if @ingester
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      body = chunk_to_body(chunk)
      response = send_request(body)
      raise "Encountered server error #{response.body}" if response.code >= 400

      response.flush
    end

    private

    def chunk_to_body(chunk)
      data = []

      chunk.msgpack_each do |(tag, time, record)|
        line = gather_line_data(tag, time, record)
        data << line unless line[:line].empty?
      end

      { lines: data }
    end

    def gather_line_data(_tag, _time, record)
      line = {
        host: record["host"],
        line: record["log"],
        level: "info",
        app: record["app"] || record["database"],
        meta: {
          file: record["file"],
          service: record["service"],
          container: record["container"],
          stream: record["stream"],
          version: record["@version"],
          source: record["source"],
          host: record["host"],
          offset: record["offset"],
          layer: record["layer"],
        }
      }

      %w[log host app level].each { |k| record.delete(k) }
      line[:meta].merge!(record.transform_keys(&:to_sym))

      # Any fields that are passed in as tags should
      # be overriden by the tags
      line.merge!(@tags_hash) if @tags

      line
    end

    def send_request(body)
      now = Time.now.to_i
      params = "?hostname=#{@host}&now=#{now}"
      params = "#{params}&#{@tags}" if @tags
      url = "#{@ingester_endpoint}#{params}"

      @ingester.headers("apikey" => @api_key,
                        "content-type" => "application/json")
               .timeout(connect: @request_timeout, write: @request_timeout, read: @request_timeout)
               .post(url, json: body)
    end
  end
end
