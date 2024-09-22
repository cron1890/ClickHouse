#include <Processors/Formats/Impl/CSV2RowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>

#include <IO/WriteHelpers.h>


namespace DB
{


CSV2RowOutputFormat::CSV2RowOutputFormat(WriteBuffer & out_, const Block & header_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    data_types.resize(columns);
    for (size_t i = 0; i < columns; ++i)
        data_types[i] = sample.safeGetByPosition(i).type;
}

void CSV2RowOutputFormat::writeLine(const std::vector<String> & values)
{
    for (size_t i = 0; i < values.size(); ++i)
    {
        writeCSV2String(values[i], out); // 
        if (i + 1 != values.size())
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void CSV2RowOutputFormat::writePrefix()
{
    const auto & sample = getPort(PortKind::Main).getHeader();

    if (with_names)
        writeLine(sample.getNames());

    if (with_types)
        writeLine(sample.getDataTypeNames());
}


void CSV2RowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextCSV2(column, row_num, out, format_settings);
}


void CSV2RowOutputFormat::writeFieldDelimiter()
{
    writeChar(format_settings.csv2.delimiter, out);
}


void CSV2RowOutputFormat::writeRowEndDelimiter()
{
    if (format_settings.csv2.crlf_end_of_line)
        writeChar('\r', out);
    writeChar('\n', out);
}

void CSV2RowOutputFormat::writeBeforeTotals()
{
    writeChar('\n', out);
}

void CSV2RowOutputFormat::writeBeforeExtremes()
{
    writeChar('\n', out);
}


void registerOutputFormatCSV2(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format_name, [with_names, with_types](
                   WriteBuffer & buf,
                   const Block & sample,
                   const FormatSettings & format_settings)
        {
            return std::make_shared<CSV2RowOutputFormat>(buf, sample, with_names, with_types, format_settings);
        });
        factory.markOutputFormatSupportsParallelFormatting(format_name);
    };

    registerWithNamesAndTypes("CSV2", register_func);
}

}
