##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowYAMLConfig
  #
  ##########################################
  class WorkflowYAMLConfig
    require 'yaml'
    require 'fileutils'
    require 'workflowmgr/utilities'

    DEFAULT_CONFIG = {
      DatabaseType: "SQLite3",
      WorkflowDocType: "XML",
      DatabaseServer: true,
      BatchQueueServer: true,
      WorkflowIOServer: true,
      MaxUnknowns: 3,
      MaxLogDays: 7,
      AutoVacuum: true,
      VacuumPurgeDays: 30,
      SubmitThreads: 8,
      JobQueueTimeout: 45,
      JobAcctTimeout: 45,
      # How old a lock held by another host must be before this run will probe
      # whether its owner is still alive and, if not, steal it. Only the
      # remote-host path waits: a lock held on this host is checked at once,
      # because the owner's pid can be looked up locally.
      StaleLockTimeout: 300
    }.freeze

    ##########################################
    #
    # Initialize
    #
    ##########################################
    def initialize
      # Path to configuration file is $HOME/.rocoto/$VERSION/rocotorc
      @config_dir = "#{ENV['HOME']}/.rocoto/#{WorkflowMgr.version}"
      @config_log = "#{@config_dir}/#{WorkflowMgr::WORKFLOW_ID.sub(/.xml$/, '')}"
      @config_tmp = "#{@config_dir}/tmp"
      @config_file = "#{@config_dir}/rocotorc"

      # Load the configuration

      # Create a .rocoto directory if one does not already exist
      FileUtils.mkdir_p(@config_dir) unless File.exist?(@config_dir)

      # Create a .rocoto log directory for this workflow if one does not already exist
      FileUtils.mkdir_p(@config_log) unless File.exist?(@config_log)

      # Create a .rocoto tmp dir if one does not already exist
      FileUtils.mkdir_p(@config_tmp) unless File.exist?(@config_tmp)

      # Move the legacy .wfmrc file to rocotorc file if it exists
      FileUtils.mv("#{ENV['HOME']}/.wfmrc", @config_file) if File.exist?("#{ENV['HOME']}/.wfmrc")

      # Load the rocotorc config if one exists
      if File.exist?(@config_file) && !File.zero?(@config_file)
        config = YAML.safe_load_file(@config_file, permitted_classes: [Symbol], symbolize_names: true)
        if config.is_a?(Hash)
          # Merge default config into rocotorc config if there are unspecified config options
          if config.keys.collect(&:to_s).sort != DEFAULT_CONFIG.keys.collect(&:to_s).sort
            config = DEFAULT_CONFIG.merge(config).delete_if { |k, v| !DEFAULT_CONFIG.key?(k) }
            File.open("#{@config_file}.#{Process.pid}", "w") { |f| YAML.dump(config, f) }
          end
          @config = config
        else
          WorkflowMgr.log("WARNING! Reverted corrupted configuration in #{@config_file} to default.")
          WorkflowMgr.stderr("WARNING! Reverted corrupted configuration in #{@config_file} to default.")
          File.open("#{@config_file}.#{Process.pid}", "w") { |f| YAML.dump(DEFAULT_CONFIG, f) }
          @config = DEFAULT_CONFIG
        end
      else
        # Create a rocotorc file with default settings if it does not exist
        File.open("#{@config_file}.#{Process.pid}", "w") { |f| YAML.dump(DEFAULT_CONFIG, f) }
        @config = DEFAULT_CONFIG
      end

      # Update the config file in a quasi-atomic way.
      FileUtils.mv("#{@config_file}.#{Process.pid}", @config_file) if File.exist?("#{@config_file}.#{Process.pid}")
    end

    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name, *args)
      configkey = name.to_sym
      if @config.key?(configkey)
        @config[configkey]
      else
        super
      end
    end

    def respond_to_missing?(name, include_private = false)
      @config.key?(name.to_sym) || super
    end
  end
end
