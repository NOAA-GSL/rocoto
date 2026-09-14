##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  require 'workflowmgr/workflowoption'
  require 'workflowmgr/selectionutil'

  ##########################################
  #
  # Class WorkflowSubsetOptions
  #
  ##########################################
  class WorkflowSubsetOptions < WorkflowOption
    require 'workflowmgr/workflowselection'

    attr_reader :database, :workflowdoc, :cycles, :tasks, :metatasks, :verbose, :harvest_only

    ##########################################
    #
    # Initialize
    #
    ##########################################
    def initialize(args, name, action, default_all: false)
      @cycles = nil
      @task_options = []
      @default_all = default_all ? true : false # true => command defaults to all tasks and cycles
      @name = name # ie.: rocotoboot
      @action = action # ie.: boot
      @all_tasks = false
      @all_cycles = false
      @harvest_only = false
      @selection = nil
      super(args)
    end

    private

    ##########################################
    #
    # add_opts
    #
    ##########################################
    def add_opts(opts)
      super(opts)

      # Override the command usage text
      opts.banner = "Usage:  #{@name} [-h] [-v #] -d database_file -w workflow_document " \
                   "[-c cycle_list] [-t task_list] [-m metatask_list] [-a] [-n]"

      # Cycles of interest
      #      C   C,C,C  C:C  :C   C:
      #        where C='YYYYMMDDHHMM', C:  >= C, :C  <= C
      opts.on("-c", "--cycles 'c1,c2,c3' | 'c1:c2' | ':c' | 'c:' | : | all", String, "List of cycles") do |clist|
        @cycles = [] if @cycles.nil?
        case clist
        when /^\d{12}(,\d{12})*$/
          @cycles.concat(clist.split(",").collect { |c| Time.gm(c[0..3], c[4..5], c[6..7], c[8..9], c[10..11]) })
        when /^(\d{12}):(\d{12})$/
          @cycles << (Time.gm(::Regexp.last_match(1)[0..3], ::Regexp.last_match(1)[4..5],
                              ::Regexp.last_match(1)[6..7], ::Regexp.last_match(1)[8..9],
                              ::Regexp.last_match(1)[10..11])..
                      Time.gm(::Regexp.last_match(2)[0..3], ::Regexp.last_match(2)[4..5],
                              ::Regexp.last_match(2)[6..7], ::Regexp.last_match(2)[8..9],
                              ::Regexp.last_match(2)[10..11]))
        when /^:(\d{12})$/
          @cycles << (Time.gm(1900, 1, 1, 0, 0)..
                      Time.gm(::Regexp.last_match(1)[0..3], ::Regexp.last_match(1)[4..5],
                              ::Regexp.last_match(1)[6..7],
                              ::Regexp.last_match(1)[8..9], ::Regexp.last_match(1)[10..11]))
        when /^(\d{12}):$/
          @cycles << (Time.gm(::Regexp.last_match(1)[0..3], ::Regexp.last_match(1)[4..5],
                              ::Regexp.last_match(1)[6..7],
                              ::Regexp.last_match(1)[8..9], ::Regexp.last_match(1)[10..11])..
                      Time.gm(9999, 12, 31, 23, 59))
        when /^all|:$/i
          @cycles << ALL_POSSIBLE_CYCLES
          @all_cycles = true
        when /^@(\S+)/
          @cycles << CycleDefSelection.new(::Regexp.last_match(1))
        else
          puts opts
          puts "Unrecognized -c option #{clist}"
          Process.exit
        end
      end

      @cycles = nil if !@cycles.nil? && @cycles.empty?

      # Tasks of interest
      opts.on("-t", "--tasks 'a,b,c'", Array, "List of tasks") do |tasklist|
        add_task_option(TaskSelection.new(tasklist))
      end

      # Metaasks of interest
      opts.on("-m", "--metatasks 'a,b,c'", Array, "List of metatasks") do |metatasklist|
        add_task_option(MetataskSelection.new(metatasklist))
      end

      # Rewind all tasks for the specified cycles instead of a list of tasks:
      opts.on("-a", '--all', "Selects all tasks.") do |flag|
        @all_tasks = true
      end

      # Harvest without making any changes to the batch system (rocotorun only)
      if @name == 'rocotorun'
        opts.on("--harvest-only", "Harvest pending submissions without making any changes to the batch system") do
          @harvest_only = true
        end
      end
    end

    ##########################################
    #
    # add_task_option
    #
    ##########################################
    def add_task_option(opt)
      @task_options << opt
    end

    ##########################################
    #
    # validate_opts
    #
    ##########################################
    def validate_opts(opts, args)
      super(opts, args)

      if @cycles.nil?
        if @default_all
          @cycles = [ALL_POSSIBLE_CYCLES]
        elsif !@allow_empty
          raise OptionParser::ParseError, "At least one cycle must be specified."
        end
      end

      if @all_tasks && !@task_options.empty?
        raise OptionParser::ParseError,
              "When providing the -a argument (all tasks), you must not provide -t or -m arguments."
      end

      if @task_options.empty? && !@all_tasks
        if @default_all
          @all_tasks = true
        elsif !@allow_empty
          raise OptionParser::ParseError,
                "At least one task or metatask (-t or -m) must be specified, or all tasks (-a)."
        end
      end
    end

    public

    ##########################################
    #
    # all_tasks
    #
    ##########################################
    def all_tasks
      return true if @all_tasks
      return default_all if @task_options.empty?

      false
    end

    ##########################################
    #
    # all_cycles
    #
    ##########################################
    def all_cycles
      return default_all if @cycles.nil?

      @cycles.include? ALL_POSSIBLE_CYCLES
    end

    ##########################################
    #
    # selection
    #
    ##########################################
    def selection
      if @selection.nil?
        @selection = WorkflowSelection.new(@all_tasks, @task_options, @cycles, default_all: @default_all)
      end
      @selection
    end
  end
end
