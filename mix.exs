defmodule ExMemcached.Mixfile do
  use Mix.Project

  def project do
    [ app: :ex_memcached,
      version: "0.0.1",
      elixir: "~> 0.13.0",
      deps: deps,
      elixirc_options: options(Mix.env) ]
  end

  # Configuration for the OTP application
  def application do
    [
      mod: { ExMemcached, [] },
      applications: [:xgen, :exlager, :ranch],
      # application configuration goes here:
      env: [
        listen_port: 8080,
        max_data_size: 1024*1024,
        max_connections: 1024,
        disable_flush_all: false
      ]
    ]
  end

  # Returns the list of dependencies in the format:
  # { :foobar, git: "https://github.com/elixir-lang/foobar.git", tag: "0.1" }
  #
  # To specify particular versions, regardless of the tag, do:
  # { :barbat, "~> 0.1", github: "elixir-lang/barbat" }
  defp deps do
    [
      { :xgen, github: "josevalim/xgen" },
      { :exlager, github: "khia/exlager" },
      { :ranch, github: "extend/ranch" },
      {:exrm, github: "bitwalker/exrm"}
    ]
  end

  defp options(env) when env in [:dev, :test] do
    [exlager_level: :debug, exlager_truncation_size: 8096]
  end
  defp options(env) when env in [:prod] do
    [exlager_level: :error, exlager_truncation_size: 8096]
  end
end
