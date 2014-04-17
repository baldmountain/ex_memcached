defmodule MemcachedE.Mixfile do
  use Mix.Project

  def project do
    [ app: :memcached_e,
      version: "0.0.1",
      elixir: "~> 0.13.0-dev",
      deps: deps,
      elixirc_options: options(Mix.env) ]
  end

  # Configuration for the OTP application
  def application do
    [
      mod: { MemcachedE, [] },
      applications: [:xgen, :exlager]
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
      { :exlager, github: "khia/exlager" }
    ]
  end

  defp options(env) when env in [:dev, :test] do
    [exlager_level: :debug, exlager_truncation_size: 8096]
  end
end
