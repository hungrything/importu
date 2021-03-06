require 'multi_json'

class Importu::Importer::Json < Importu::Importer
  def initialize(infile, options = {})
    super

    begin
      infile.rewind
      @reader = MultiJson.load(infile.read)
    rescue MultiJson::DecodeError => e
      raise Importu::InvalidInput, e.message
    end
  end

  def import!(finder_scope = nil, &block)
    result = super
    outfile.write(JSON.pretty_generate(@error_records)) if @invalid > 0
    result
  end

  def records(&block)
    enum = Enumerator.new do |yielder|
      @reader.each_with_index do |data,idx|
        yielder.yield record_class.new(self, data, data)
      end
    end
  end

  def import_record(record, finder_scope, &block)
    begin
      super
    rescue Importu::InvalidRecord => e
      write_error(record.raw_data, e.message)
    end
  end


  private

  def write_error(data, msg)
    @error_records ||= []
    @error_records << data.merge('_errors' => msg)
  end

end
