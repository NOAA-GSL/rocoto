##########################################
#
# module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class WorkflowIO
  #
  ##########################################
  class WorkflowIO
    require 'fileutils'


    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize; end

    ##########################################
    #
    # read
    #
    ##########################################
    def read(filename)
      File.read(filename)
    end

    ##########################################
    #
    # exists?
    #
    ##########################################
    def exist?(filename)
      File.exist?(filename)
    end

    ##########################################
    #
    # mtime
    #
    ##########################################
    def mtime(filename)
      File.mtime(filename)
    end

    ##########################################
    #
    # size
    #
    ##########################################
    def size(filename)
      File.size(filename)
    end

    ##########################################
    #
    # mkdir_p
    #
    ##########################################
    def mkdir_p(dirname)
      FileUtils.mkdir_p(dirname)
    end

    ##########################################
    #
    # log
    #
    ##########################################
    def log(logname, msg)
      host = Socket.gethostname
      logdir = File.dirname(logname)
      FileUtils.mkdir_p(logdir)
      File.open(logname, "a+") do |logfile|
        logfile.puts("#{Time.now} :: #{host} :: #{msg}")
      end
    end

    ##########################################
    #
    # roll_log
    #
    ##########################################
    def roll_log(logname)
      # If the log file exists, roll it
      if File.exist?(logname)

        Dir["#{logname}*"].sort.reverse.each do |f|
          if f =~ /#{logname}\.(\d+)$/
            ext = ::Regexp.last_match(1).to_i
            FileUtils.mv(f, "#{logname}.#{ext + 1}")
          else
            FileUtils.mv(f, "#{logname}.0")
          end
        end

      end
    end
  end
end
