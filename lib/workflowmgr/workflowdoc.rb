##########################################
#
# Module WorkflowMgr
#
##########################################
require 'English'
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowXMLDoc
  #
  ##########################################
  class WorkflowXMLDoc
    require 'time'

    require 'nokogiri'
    require 'workflowmgr/utilities'
    require 'workflowmgr/cycledef'
    require 'workflowmgr/workflowlog'
    require 'workflowmgr/moabbatchsystem'
    require 'workflowmgr/moabtorquebatchsystem'
    require 'workflowmgr/torquebatchsystem'
    require 'workflowmgr/pbsprobatchsystem'
    require 'workflowmgr/lsfbatchsystem'
    require 'workflowmgr/lsfcraybatchsystem'
    require 'workflowmgr/slurmbatchsystem'
    require 'workflowmgr/task'

    @@messages = {
      exclusive: "Ignoring <exclusive>; this platform does not support it.",
      shared: "Ignoring <shared>; this platform does not support it."
    }

    def unescape(xmlstr)
      # This is a workaround for a LibXML bug: it is impossible to
      # disable output escaping.  That means strings will have &quot;
      # instead of " no matter what you do.  This function replaces
      # some common XML entities with their corresponding values.
      t = xmlstr.gsub(/&quot;/, '"').gsub(/&lt;/, '<').gsub(/&gt;/, '>')

      # The &amp; must be last:
      t.gsub(/&amp;/, '&')
    end

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(workflowdoc, workflow_io_server, config)
      # Set the configuration
      @config = config

      # Set the workflow_io_server
      @workflow_io_server = workflow_io_server

      # Track features used in the document that are only supported by
      # some platforms:
      @features_used = {}

      begin
        if @workflow_io_server.exist?(workflowdoc)
          @workflowdoc = Nokogiri::XML(@workflow_io_server.read(workflowdoc), workflowdoc) do |xml_config|
            xml_config.noent
            xml_config.nonet
            xml_config.huge
            xml_config.nocdata
          end
          unless @workflowdoc.errors.empty?
            parse_errors = @workflowdoc.errors.map(&:to_s).join("\n")
            raise "Cannot parse workflow document #{workflowdoc} because it is malformed:\n#{parse_errors}"
          end
        else
          raise "Cannot read XML file, #{workflowdoc}, because it does not exist!"
        end
      rescue WorkflowIOHang
        WorkflowMgr.log($ERROR_INFO.to_s)
        WorkflowMgr.stderr($ERROR_INFO.to_s, 2)
        raise "ERROR! Cannot read file, #{workflowdoc}, because it resides on an unresponsive filesystem"
      end

      # Validate the workflow xml document before metatask expansion
      validate_with_metatasks(@workflowdoc)

      # Expand metatasks
      expand_metatasks

      # Expand metatask dependencies
      expand_metataskdeps

      # Insert dependencies for auto-serialized metatasks
      expand_serialdeps

      # Validate the workflow xml document after metatask expansion
      # The second validation is needed in case metatask expansion introduced invalid XML
      validate_without_metatasks(@workflowdoc)
    end

    ##########################################
    #
    # realtime?
    #
    ##########################################
    def realtime?
      realtime = @workflowdoc.root["realtime"]
      if realtime.nil?
        nil
      else
        !(realtime.downcase =~ /^t|true$/).nil?
      end
    end

    ##########################################
    #
    # cyclelifespan
    #
    ##########################################
    def cyclelifespan
      cls = @workflowdoc.root["cyclelifespan"]
      if cls.nil?
        nil
      else
        WorkflowMgr.ddhhmmss_to_seconds(cls)
      end
    end

    ##########################################
    #
    # cyclethrottle
    #
    ##########################################
    def cyclethrottle
      ct = @workflowdoc.root["cyclethrottle"]
      if ct.nil?
        nil
      else
        ct.to_i
      end
    end

    ##########################################
    #
    # taskthrottle
    #
    ##########################################
    def taskthrottle
      tt = @workflowdoc.root["taskthrottle"]
      if tt.nil?
        nil
      else
        tt.to_i
      end
    end

    ##########################################
    #
    # metatask_throttles
    #
    ##########################################
    attr_reader :metatask_throttles

    ##########################################
    #
    # corethrottle
    #
    ##########################################
    def corethrottle
      ct = @workflowdoc.root["corethrottle"]
      if ct.nil?
        nil
      else
        ct.to_i
      end
    end

    ##########################################
    #
    # scheduler
    #
    ##########################################
    def scheduler
      sched = @workflowdoc.root["scheduler"]
      if sched.nil?
        nil
      else
        WorkflowMgr.const_get("#{sched.upcase}BatchSystem").new(@config)
      end
    end

    ##########################################
    #
    # scheduler_name
    #
    # The scheduler as the document names it. A batch system object cannot
    # cross to the process that serves it, so what travels is this name and
    # the two timeouts its constructor reads.
    #
    ##########################################
    def scheduler_name
      @workflowdoc.root["scheduler"]
    end

    ##########################################
    #
    # features_supported?
    #
    ##########################################
    def features_supported?
      supported = false
      sched = @workflowdoc.root["scheduler"]
      unless sched.nil?
        clazz = WorkflowMgr.const_get("#{sched.upcase}BatchSystem")
        supported = true
        @features_used.each_key do |feature|
          next if clazz.feature?(feature)

          supported = false
          WorkflowMgr.stderr(@@messages[feature] || "Feature '#{feature}' is unsupported on this platform.")
        end
      end
      supported
    end

    ##########################################
    #
    # log
    #
    ##########################################
    def log
      lognode = @workflowdoc.at_xpath('/workflow/log')
      path = get_compound_time_string(lognode)
      verbosity = lognode['verbosity']
      verbosity = verbosity.to_i unless verbosity.nil?

      WorkflowLog.new(path, verbosity, @workflow_io_server)
    end

    ##########################################
    #
    # cycledefs
    #
    ##########################################
    def cycledefs
      cycles = []
      cyclenodes = @workflowdoc.xpath('/workflow/cycledef')
      cyclenodes.each do |cyclenode|
        # output_escaping removed: Nokogiri does not escape content by default
        cyclefields = unescape(cyclenode.content.strip)
        nfields = cyclefields.split.size
        group = cyclenode['group']
        activation_offset = if realtime?
                              WorkflowMgr.ddhhmmss_to_seconds(cyclenode['activation_offset'])
                            else
                              0
                            end
        exclude_hours = cyclenode['exclude_hours']
        valid_hours = cyclenode['valid_hours']
        if nfields == 3
          cycles << CycleInterval.new(cyclefields, group, activation_offset, nil, exclude_hours, valid_hours)
        elsif nfields == 6
          if exclude_hours || valid_hours
            raise "ERROR: 'exclude_hours' or 'valid_hours' not supported in cron-style <cycledef>."
          end

          cycles << CycleCron.new(cyclefields, group, activation_offset)
        else
          raise "ERROR: Unsupported <cycle> type!"
        end
      end

      cycles
    end

    ##########################################
    #
    # tasks
    #
    ##########################################
    def tasks
      tasks = {}
      tasknodes = @workflowdoc.xpath('/workflow/task')
      tasknodes.each_with_index do |tasknode, seq|
        natives = []
        rewinders = []
        taskattrs = {}
        taskenvars = {}
        taskdep = nil
        taskhangdep = nil
        # Get task attributes insde the <task> tag
        tasknode.attributes.each_value do |attr|
          attrkey = attr.name.to_sym
          attrval = case attrkey
                    when :maxtries, :throttle # Attributes with integer values go here
                      attr.value.to_i
                    when :final # Attributes with boolean values go here
                      !(attr.value.downcase =~ /^t|true$/).nil?
                    else # Attributes with string values
                      attr.value
                    end
          taskattrs[attrkey] = attrval
        end

        # Get task attributes, envars, and dependencies declared as elements inside <task> element
        tasknode.element_children.each do |e|
          case e.name
          when /shared/
            taskattrs[:shared] = true
            @features_used[:shared] = true
          when /exclusive/
            taskattrs[:exclusive] = true
            @features_used[:exclusive] = true
          when /^envar$/
            envar_name = nil
            envar_value = nil
            e.element_children.each do |element|
              case element.name
              when /^name$/
                envar_name = get_compound_time_string(element)
              when /^value$/
                envar_value = get_compound_time_string(element)
              end
            end
            taskenvars[envar_name] = envar_value
          when /^rewind$/
            e.element_children.each do |element|
              if element.name == 'rb'
                rewinders.push(get_rubydep(element))
              elsif element.name == 'sh'
                rewinders.push(get_shelldep(element))
              else
                raise "Invalid tag <#{element.name}> inside <rewind> tag: #{element.node_type_name}"
              end
            end
          when /^dependency$/
            e.element_children.each do |element|
              raise "ERROR: <dependency> tag contains too many elements" unless taskdep.nil?

              taskdep = Dependency.new(get_dependency_node(element))
            end
          when /^hangdependency$/
            e.element_children.each do |element|
              raise "ERROR: <hangdependency> tag contains too many elements" unless taskhangdep.nil?

              taskhangdep = Dependency.new(get_dependency_node(element))
            end
          else
            attrkey = e.name.to_sym
            case attrkey
            when :cores # <task> elements with integer values go here
              # output_escaping removed: Nokogiri does not escape content by default
              attrval = unescape(e.content).to_i
            when :nodes
              # output_escaping removed: Nokogiri does not escape content by default
              attrval = unescape(e.content)
              taskattrs[:cores] = 0
              attrval.split("+").each do |nodespec|
                resources = nodespec.split(":")
                nodes = resources.shift.to_i
                tpp = 1
                ppn = 0
                resources.each do |resource|
                  case resource
                  when /ppn=(\d+)/
                    ppn = ::Regexp.last_match(1).to_i
                  when /tpp=(\d+)/
                    tpp = ::Regexp.last_match(1).to_i
                  end
                end
                raise "ERROR: <node> tag must contain a :ppn setting for each nodespec" if ppn == 0

                taskattrs[:cores] += nodes * ppn * tpp
              end
            when :nodesize
              # output_escaping removed: Nokogiri does not escape content by default
              attrval = unescape(e.content).to_i
            else # <task> elements with compoundtimestring values
              attrval = get_compound_time_string(e)
            end
            if attrkey.to_s == 'native'
              natives.push(attrval)
            else
              taskattrs[attrkey] = attrval
            end
          end
        end

        task = Task.new(seq, taskattrs, taskenvars, taskdep, taskhangdep)
        tasks[task.attributes[:name]] = task
        rewinders.each do |rewinder|
          task.add_rewind_action(rewinder)
        end
        inv = 0
        natives.each do |native|
          task.add_native(native)
          inv += 1
        end

        knnv = 0
        task.each_native do |native|
          knnv += 1
        end
        if knnv != inv
          raise "ERROR: Ruby did not add all objects to the internal list: #{knnv}!=#{inv}"
        end
      end

      tasks
    end

    ##########################################
    #
    # taskdep_cycle_offsets
    #
    ##########################################
    def taskdep_cycle_offsets
      offsets = []
      taskdepnodes = @workflowdoc.xpath('//taskdep')
      taskdepnodes.each do |taskdepnode|
        offsets << WorkflowMgr.ddhhmmss_to_seconds(taskdepnode["cycle_offset"]) \
          unless taskdepnode["cycle_offset"].nil?
      end

      taskdepnodes = @workflowdoc.xpath('//cycleexistdep')
      taskdepnodes.each do |taskdepnode|
        offsets << WorkflowMgr.ddhhmmss_to_seconds(taskdepnode["cycle_offset"]) \
          unless taskdepnode["cycle_offset"].nil?
      end

      offsets.uniq
    end

    private


    ##########################################
    #
    # get_compound_time_string
    #
    ##########################################
    def get_compound_time_string(element)
      if element.nil?
        return nil
      end

      strarray = []
      element.children.each do |e|
        # output_escaping removed: Nokogiri does not escape content by default
        if e.text?
          strarray << unescape(e.content)
        elsif e.comment?
          # Ignore comments
        else
          offset_sec = WorkflowMgr.ddhhmmss_to_seconds(e["offset"])
          case e.name
          when "cyclestr"
            formatstr = unescape(e.content)
            formatstr.gsub!(/%/, '%%')
            formatstr.gsub!(/@(\^?[^@\s])/, '%\1')
            formatstr.gsub!(/@@/, '@')
            strarray << CycleString.new(formatstr, offset_sec)
          else
            raise "Invalid tag <#{e.name}> inside #{element}: #{e.node_type_name}"
          end
        end
      end

      CompoundTimeString.new(strarray)
    end

    ##########################################
    #
    # get_dependency_node
    #
    ##########################################
    def get_dependency_node(element)
      # Build a dependency tree
      children = []
      element.element_children.each { |e| children << e }
      case element.name
      when "not"
        DependencyNotOperator.new(children.collect { |child| get_dependency_node(child) })
      when "and"
        DependencyAndOperator.new(children.collect { |child| get_dependency_node(child) })
      when "or"
        DependencyOrOperator.new(children.collect { |child|  get_dependency_node(child) })
      when "nand"
        DependencyNandOperator.new(children.collect { |child| get_dependency_node(child) })
      when "nor"
        DependencyNorOperator.new(children.collect { |child|  get_dependency_node(child) })
      when "xor"
        DependencyXorOperator.new(children.collect { |child|  get_dependency_node(child) })
      when "some"
        DependencySomeOperator.new(children.collect do |child|
          get_dependency_node(child)
        end, element["threshold"].to_f)
      when "taskdep"
        get_taskdep(element)
      when "streq", "strneq", "true", "false"
        get_constdep(element)
      when "rb"
        get_rubydep(element)
      when "sh"
        get_shelldep(element)
      when "cycleexistdep"
        get_cycleexistdep(element)
      when "taskvalid"
        get_taskvaliddep(element)
      when "datadep"
        get_datadep(element)
      when "timedep"
        get_timedep(element)
      end
    end

    #####################################################
    #
    # get_constdep
    #
    #####################################################
    def get_constdep(element)
      case element.name
      when 'true'
        ConstDependency.new(true, 'true')
      when 'false'
        ConstDependency.new(false, 'false')
      when 'streq', 'strneq'
        left = get_compound_time_string(element.xpath('left').first)
        right = get_compound_time_string(element.xpath('right').first)
        if element.name == 'streq'
          name = name_stringdep(left, right, '==')
          compare = '=='
        else
          name = name_stringdep(left, right, "!=")
          compare = '!='
        end
        StringDependency.new(left, right, name, compare)
      else
        raise "Invalid constant dependency #{element.name}"
      end
    end

    #####################################################
    #
    # name_stringdep
    #
    #####################################################

    def name_stringdep(left, right, cmp)
      ia = left.inspect
      ib = right.inspect
      cmp = cmp.to_s
      ia = "#{ia[0..37]}..." if ia.size > 40
      ib = "#{ib[0..37]}..." if ib.size > 40
      cmp = "#{cmp[0..37]}..." if cmp.size > 40
      "'#{ia}'#{cmp}'#{ib}'"
    end

    #####################################################
    #
    # get_taskdep
    #
    #####################################################
    def get_taskdep(element)
      # Get the mandatory task attribute
      task = element["task"]

      # Get the state attribute
      state = element["state"] || "SUCCEEDED"

      # Get the cycle offset, if there is one
      cycle_offset = WorkflowMgr.ddhhmmss_to_seconds(element["cycle_offset"]) || 0

      TaskDependency.new(task, state, cycle_offset)
    end

    #####################################################
    #
    # get_cycleexistdep
    #
    #####################################################
    def get_cycleexistdep(element)
      # Get the cycle offset, if there is one
      cycle_offset = WorkflowMgr.ddhhmmss_to_seconds(element["cycle_offset"])

      CycleExistDependency.new(cycle_offset)
    end

    #####################################################
    #
    # get_taskvaliddep
    #
    #####################################################
    def get_taskvaliddep(element)
      task = element["task"]

      TaskValidDependency.new(task)
    end

    #####################################################
    #
    # get_rubydep
    #
    #####################################################
    def get_rubydep(element)
      # Get the cycle offset, if there is one
      name = element["name"]
      text = get_compound_time_string(element)
      RubyDependency.new(text, name)
    end

    #####################################################
    #
    # get_shelldep
    #
    #####################################################
    def get_shelldep(element)
      # Get the cycle offset, if there is one
      name = element["name"]
      shell = element["shell"]
      runopt = element["runopt"]
      if shell.nil?
        shell = '/bin/sh' # POSIX requires this location for the POSIX sh
      end
      if runopt.nil?
        runopt = '-c' # sh -c (command)   -- need the -c
      end
      text = get_compound_time_string(element)
      ShellDependency.new(shell, runopt, text, name)
    end

    ##########################################
    #
    # get_datadep
    #
    ##########################################
    def get_datadep(element)
      # Get the age attribute
      age_sec = WorkflowMgr.ddhhmmss_to_seconds(element["age"]) || 0

      # Get the minsize attribute
      minsize = element["minsize"] || 0
      case minsize
      when /^(\d+)$/
        minsize = ::Regexp.last_match(1).to_i
      when /^(\d+)[B|b]$/
        minsize = ::Regexp.last_match(1).to_i
      when /^(\d+)[K|k]$/
        minsize = ::Regexp.last_match(1).to_i * 1024
      when /^(\d+)[M|m]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024
      when /^(\d+)[G|g]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024
      when /^(\d+)[T|t]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024 * 1024
      when /^(\d+)[P|p]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024 * 1024 * 1024
      when /^(\d+)[E|e]$/
        minsize = ::Regexp.last_match(1).to_i * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
      end

      DataDependency.new(get_compound_time_string(element), age_sec, minsize)
    end

    #####################################################
    #
    # get_timedep
    #
    #####################################################
    def get_timedep(element)
      # Get the time cycle string
      TimeDependency.new(get_compound_time_string(element))
    end

    ##########################################
    #
    # validate_with_metatasks
    #
    ##########################################
    def validate_with_metatasks(doc)
      # Parse the Relax NG schema XML document
      relaxng_schema = Nokogiri::XML::RelaxNG(@workflow_io_server.read("#{File.dirname(__FILE__)}/schema_with_metatasks.rng"))

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      errors = relaxng_schema.validate(doc)
      raise "Validation failed: #{errors.map(&:to_s).join("\n")}" unless errors.empty?
    end

    ##########################################
    #
    # validate_without_metatasks
    #
    ##########################################
    def validate_without_metatasks(doc)
      # Parse the Relax NG schema XML document
      relaxng_schema = Nokogiri::XML::RelaxNG(@workflow_io_server.read("#{File.dirname(__FILE__)}/schema_without_metatasks.rng"))

      # Validate the workflow XML file against the general Relax NG Schema that validates metatask tags
      errors = relaxng_schema.validate(doc)
      raise "Validation failed: #{errors.map(&:to_s).join("\n")}" unless errors.empty?
    end

    ##########################################
    #
    # expand_metataskdeps
    #
    ##########################################
    def expand_metataskdeps
      @workflowdoc.root.element_children.each do |ch|
        next unless ch.name == "task"

        # Find the metataskdep nodes
        metataskdeps = ch.xpath('.//metataskdep')

        # Replace each of them with the equivalient <and><taskdep/><taskdep/>...</and> expression
        metataskelements = []
        metataskdeps.each do |metataskdep|
          metataskelements << metataskdep

          # Get the name of the metatask
          metatask = metataskdep["metatask"]

          # Find all names of tasks descended from the metatask
          tasknames = []
          tasks = @workflowdoc.xpath("//task[contains(@metatasks,'#{metatask}')]")
          tasks.each do |task|
            tasknames << task["name"] if task["metatasks"] =~ /^([^,]+,)*#{metatask}(,[^,]+)*$/
          end

          # Insert a "some" element after the metataskdep element
          somenode = Nokogiri::XML::Node.new("some", @workflowdoc)
          threshold = metataskdep["threshold"].nil? ? "1.0" : metataskdep["threshold"]
          somenode["threshold"] = threshold
          metataskdep.add_next_sibling(somenode)


          # Add taskdep elements as children to the and element
          tasknames.each do |task|
            taskdepnode = Nokogiri::XML::Node.new("taskdep", @workflowdoc)
            taskdepnode["task"] = task
            unless metataskdep["cycle_offset"].nil?
              taskdepnode["cycle_offset"] = metataskdep["cycle_offset"]
            end
            unless metataskdep["state"].nil?
              taskdepnode["state"] = metataskdep["state"]
            end
            somenode << taskdepnode
          end
        end

        # Remove the metataskdep elements
        metataskelements.each(&:remove)
      end
    end

    ##########################################
    #
    # expand_serialdeps
    #
    ##########################################
    def expand_serialdeps
      @workflowdoc.root.element_children.each do |ch|
        next unless ch.name == "task"

        # Skip tasks that are not members of a metatask
        next if ch["metatasks"].nil?

        depnode = nil
        andnode = nil

        # Add dependencies for each serial metatask that this task is a member of
        metatasklist = ch["metatasks"]
        metatasklist.split(",").each_with_index do |m, idx|
          # Ignore parallel metatasks
          next unless @metatask_modes[m] == "serial"

          # Find the seqnum for the tasks on which this task depends
          seqdeplist = ch["seqnum"]
          seqdeps = seqdeplist.split(",")[0..idx].collect(&:to_i)
          seqdeps[idx] -= 1

          # Unless this is the first task in the sequence, it has a dependency for this metatask
          next unless seqdeps[idx] > 0

          # Find the <dependency> node for this task, or make one if it isn't found
          if depnode.nil?
            depnode = ch.at_xpath("./dependency")
            if depnode.nil?
              depnode = Nokogiri::XML::Node.new("dependency", @workflowdoc)
              andnode = Nokogiri::XML::Node.new("and", @workflowdoc)
              ch << depnode
              depnode << andnode
            else
              depchild = depnode.at_xpath("./*[1]")
              if depchild.name == "and"
                andnode = depchild
              else
                depchildren = depnode.children
                andnode = Nokogiri::XML::Node.new("and", @workflowdoc)
                depnode << andnode
                depchildren.each do |c|
                  andnode << c
                end
              end
            end
          end

          # Find all tasks that match the sequence number for dependent tasks
          tasks1 = @workflowdoc.xpath("//task[starts-with(@seqnum,'#{seqdeps.join(',')},')]")
          tasks2 = @workflowdoc.xpath("//task[@seqnum='#{seqdeps.join(',')}']")
          tasks = tasks1.to_a | tasks2.to_a

          # Insert a task dep for each dependent task
          tasks.each do |t|
            # Reject tasks that aren't a member of metatask m
            next if t["metatasks"].split(",").find_index(m).nil?

            taskdepnode = Nokogiri::XML::Node.new("taskdep", @workflowdoc)
            taskdepnode["task"] = t["name"]
            andnode << taskdepnode
          end
        end
      end
    end

    ##########################################
    #
    # expand_metatasks
    #
    ##########################################
    def expand_metatasks
      # Parse and expand metatasks
      metatasks = []
      @metatask_seq = 1
      @metatask_throttles = {}
      @metatask_modes = {}
      @workflowdoc.root.element_children.each do |ch|
        next unless ch.name == "metatask"

        if ch["name"].nil?
          ch["name"] = "metatask#{@metatask_seq}"
          @metatask_seq += 1
        end
        metatask_name = ch["name"]
        @metatask_throttles[metatask_name] = ch["throttle"].nil? ? 999_999 : ch["throttle"].to_i
        @metatask_modes[metatask_name] = ch["mode"].nil? ? "parallel" : ch["mode"]
        pre_parse(ch, metatask_name)
        metatasks << ch
      end
      metatasks.each(&:remove)
    end

    #####################################################
    #
    # traverse
    #
    #####################################################
    def traverse(node, id_table, index)
      # if node.node_type_name == "text"
      if node.text?
        cont = unescape(node.content)
        id_table.each_key do |id|
          next while cont.sub!("##{id}#", id_table[id][index])
        end
        node.content = cont

      else
        node.attributes.each_value do |attr|
          val = attr.value
          id_table.each_key do |id|
            next while val.sub!("##{id}#", id_table[id][index])
          end
          attr.value = val
        end
        node.children.each { |ch| traverse(ch, id_table, index) }
      end
    end

    #####################################################
    #
    # pre-parse
    #
    #####################################################
    def pre_parse(metatask, metatask_list)
      id_table = {}
      var_length = -1

      # Set the metatask list for all task children of this metatask
      seqnum = 0
      if metatask["seqnum"].nil?
        metatask["seqnum"] = ""
      else
        metatask["seqnum"] += ","
      end
      metatask.children.each do |e|
        if e.name == "task"
          e["metatasks"] = metatask_list
          e["seqnum"] = metatask["seqnum"]
          seqnum += 1
          e["seqnum"] += seqnum.to_s
        elsif e.name == "metatask"
          e["seqnum"] = metatask["seqnum"]
          seqnum += 1
          e["seqnum"] += seqnum.to_s
        end
      end

      # Parse each metatask child of this metatask, adding the metatask child to the metatask list
      # rubocop:disable Style/CombinableLoops
      metatask.children.each do |ch|
        next unless ch.name == "metatask"

        if ch["name"].nil?
          ch["name"] = "metatask#{@metatask_seq}"
          @metatask_seq += 1
        end
        metatask_name = ch["name"]
        @metatask_throttles[metatask_name] = ch["throttle"].nil? ? 999_999 : ch["throttle"].to_i
        @metatask_modes[metatask_name] = ch["mode"].nil? ? "parallel" : ch["mode"]
        pre_parse(ch, metatask_list + ",#{metatask_name}")
      end
      # rubocop:enable Style/CombinableLoops

      # Build a table of var tags and their values for this metatask
      # rubocop:disable Style/CombinableLoops
      metatask.children.each do |e|
        next unless e.name == "var"

        var_values = unescape(e.content).split
        var_length = var_values.length if var_length == -1
        raise "ERROR: <var> tags do not contain the same number of items!" if var_values.length != var_length

        id_table[e["name"]] = var_values
      end
      # rubocop:enable Style/CombinableLoops
      raise "ERROR: No <var> tag or values specified in one or more metatasks" if var_length < 1

      # Expand the metatasks, adding metatask list only to the expanded tasks from nested metatasks
      task_list = []
      depth = metatask_list.split(",").size
      maxseq = 0
      0.upto(var_length - 1) do |index|
        metatask.children.each do |e|
          next unless e.name == "task"

          task_copy = e.dup
          if task_copy["metatasks"].nil?
            task_copy["metatasks"] = metatask_list
          end
          traverse(task_copy, id_table, index)
          seqarr = task_copy["seqnum"].split(",")
          if index == 0
            maxseq = seqarr[depth - 1].to_i
          else
            seqarr[depth - 1] = seqarr[depth - 1].to_i + maxseq * index
            task_copy["seqnum"] = seqarr.join(",")
          end
          task_list << task_copy
        end
      end

      # Insert the expanded tasks into the XML tree
      (task_list.length - 1).downto(0) { |x| metatask.add_next_sibling(task_list[x]) }
    end
  end
end
