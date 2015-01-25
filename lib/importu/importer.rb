require 'active_record/errors'

class Importu::Importer
  attr_reader :options, :infile, :outfile, :validation_errors, :run_data
  attr_reader :total, :invalid, :created, :updated, :unchanged

  include Importu::Dsl
  include Importu::Converters

  def initialize(infile, options = {})
    @options = options
    @run_data = options.delete(:run_data)
    @total = @invalid = @created = @updated = @unchanged = 0
    @validation_errors = Hash.new(0) # counter for each validation error

    @infile = infile.respond_to?(:readline) ? infile : File.open(infile, 'rb')
  end

  def records
    [].to_enum # implement in a subclass
  end

  def outfile
    @outfile ||= Tempfile.new('import', Rails.root.join('tmp'), 'wb+')
  end

  def import!(finder_scope = nil, &block)
    # if a scope is passed in, that scope becomes the starting scope used by
    # the finder, otherwise the model's default scope is used).

    finder_scope ||= model_class.scoped
    records.each {|r| import_record(r, finder_scope, &block) }
  end

  def result_msg
    msg = <<-END.strip_heredoc
      Total:     #{@total}
      Created:   #{@created}
      Updated:   #{@updated}
      Invalid:   #{@invalid}
      Unchanged: #{@unchanged}
    END

    if @validation_errors.any?
      msg << "\nValidation Errors:\n"
      msg << @validation_errors.map {|e,c| "  - #{e}: #{c}" }.join("\n")
    end

    msg
  end


  protected

  def model_class
    @model_class ||= model.constantize
  end

  def import_record(record, finder_scope, &block)
    begin
      object = find(finder_scope, record) || model_class.new
      action = object.new_record? ? :create : :update
      check_duplicate(object) if action == :update

      case ([action] - allowed_actions).first
        when :create then raise Importu::InvalidRecord, "#{model} not found"
        when :update then raise Importu::InvalidRecord, "existing #{model} found"
      end

      record.assign_to(object, action, &block)

      case record.save!
        when :created   then @created   += 1
        when :updated   then @updated   += 1
        when :unchanged then @unchanged += 1
      end

    rescue Importu::InvalidRecord => e
      if errors = e.validation_errors
        #is this used?
        errors.each do |error| 
          @validation_errors[remove_data_from_error_message(error)] += 1
        end
      else
        @validation_errors["#{e.name}: #{remove_data_from_error_message(e.message)}"] += 1
      end

      @invalid += 1
      raise

    ensure
      @total += 1
    end
  end
  
  def remove_data_from_error_message(msg)
    # convention: assume data-specific error messages put data inside parens, e.g. 'Dupe record found (sysnum 5489x)'
    msg.gsub(/ *\([^)]+\)/,'')
  end
  
  def find(scope, record)
    field_groups = self.class.finder_fields or return
    field_groups.each do |field_group|
      if field_group.respond_to?(:call) # proc
        response = scope.instance_exec(record, &field_group)
        if response.is_a? ActiveRecord::Relation
          object = find_one_or_raise(response)
        elsif response.is_a?(model_class)  || response.nil?  #no result is valid
          object = response
        else
          raise(Importu::InvalidRecord, "find block returned a #{object.class}.  Should return ActiveRecord::Relation, model class #{model_class}, or nil")
        end
      else
        object = find_one_or_raise( scope.where( Hash[ field_group.map{ |f| [f, record[f]] } ] ) )
      end
      return object 
    end
    nil
  end
  
  def find_one_or_raise(relation)
    #Possible upgrade:  dsl :match_first_ordered_by
    if relation.count > 1
      raise(Importu::InvalidRecord, "record returned multiple matches")
    else
      object = relation.first
    end
  end
    
  

  def check_duplicate(record)
    return unless id = record.respond_to?(:id) && record.id
    if ((@encountered||=Hash.new(0))[id] += 1) > 1
      raise Importu::DuplicateRecord, 'matches a previously imported record'
    end
  end

end
