#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class WriteBuffer;


/** The stream for outputting data in csv2 format.
  * Does not conform with https://tools.ietf.org/html/rfc4180 because it uses LF, not CR LF.
  */
class CSV2RowOutputFormat final : public IRowOutputFormat
{
public:
    /** with_names - output in the first line a header with column names
      * with_types - output in the next line header with the names of the types
      */
    CSV2RowOutputFormat(WriteBuffer & out_, const Block & header_, bool with_names_, bool with_types, const FormatSettings & format_settings_);

    String getName() const override { return "CSV2RowOutputFormat"; }

    /// https://www.iana.org/assignments/media-types/text/csv2
    String getContentType() const override
    {
        return String("text/csv2; charset=UTF-8; header=") + (with_names ? "present" : "absent");
    }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;

    bool supportTotals() const override { return true; }
    bool supportExtremes() const override { return true; }

    void writeBeforeTotals() override;
    void writeBeforeExtremes() override;

    void writePrefix() override;
    void writeLine(const std::vector<String> & values);

    bool with_names;
    bool with_types;
    const FormatSettings format_settings;
    DataTypes data_types;
};

}
