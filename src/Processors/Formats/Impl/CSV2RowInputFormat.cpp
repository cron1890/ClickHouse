#include <IO/BufferWithOwnMemory.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/verbosePrintString.h>
#include <Processors/Formats/Impl/CSV2RowInputFormat.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

namespace
{
void checkBadDelimiter(char delimiter, bool allow_whitespace_or_tab_as_delimiter)
{
    if ((delimiter == ' ' || delimiter == '\t') && allow_whitespace_or_tab_as_delimiter)
    {
        return;
    }
    constexpr std::string_view bad_delimiters = " \t\"'.UL";
    if (bad_delimiters.find(delimiter) != std::string_view::npos)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "CSV2 format may not work correctly with delimiter '{}'. Try use CustomSeparated format instead",
            delimiter);
}
}

CSV2RowInputFormat::CSV2RowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : CSV2RowInputFormat(header_, std::make_shared<PeekableReadBuffer>(in_), params_, with_names_, with_types_, format_settings_)
{
}

CSV2RowInputFormat::CSV2RowInputFormat(
    const Block & header_,
    std::shared_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_,
    std::unique_ptr<CSV2FormatReader> format_reader_)
    : RowInputFormatWithNamesAndTypes(
          header_,
          *in_,
          params_,
          false,
          with_names_,
          with_types_,
          format_settings_,
          std::move(format_reader_),
          format_settings_.csv2.try_detect_header)
    , buf(std::move(in_))
{
    checkBadDelimiter(format_settings_.csv2.delimiter, format_settings_.csv2.allow_whitespace_or_tab_as_delimiter);
}

CSV2RowInputFormat::CSV2RowInputFormat(
    const Block & header_,
    std::shared_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
          header_,
          *in_,
          params_,
          false,
          with_names_,
          with_types_,
          format_settings_,
          std::make_unique<CSV2FormatReader>(*in_, format_settings_),
          format_settings_.csv2.try_detect_header)
    , buf(std::move(in_))
{
    checkBadDelimiter(format_settings_.csv2.delimiter, format_settings_.csv2.allow_whitespace_or_tab_as_delimiter);
}

void CSV2RowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(*buf);
}

void CSV2RowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    RowInputFormatWithNamesAndTypes::setReadBuffer(*buf);
}

void CSV2RowInputFormat::resetReadBuffer()
{
    buf.reset();
    RowInputFormatWithNamesAndTypes::resetReadBuffer();
}

void CSV2FormatReader::skipRow()
{
    bool quotes = false;
    ReadBuffer & istr = *buf;

    while (!istr.eof())
    {
        if (quotes)
        {
            auto * pos = find_first_symbols<'"'>(istr.position(), istr.buffer().end());
            istr.position() = pos;

            if (pos > istr.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            else if (pos == istr.buffer().end())
                continue;
            else if (*pos == '"')
            {
                ++istr.position();
                if (!istr.eof() && *istr.position() == '"')
                    ++istr.position();
                else
                    quotes = false;
            }
        }
        else
        {
            auto * pos = find_first_symbols<'"', '\r', '\n'>(istr.position(), istr.buffer().end());
            istr.position() = pos;

            if (pos > istr.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            else if (pos == istr.buffer().end())
                continue;

            if (*pos == '"')
            {
                quotes = true;
                ++istr.position();
                continue;
            }

            if (*pos == '\n')
            {
                ++istr.position();
                if (!istr.eof() && *istr.position() == '\r')
                    ++istr.position();
                return;
            }
            else if (*pos == '\r')
            {
                ++istr.position();
                if (format_settings.csv2.allow_cr_end_of_line)
                    return;
                else if (!istr.eof() && *pos == '\n')
                {
                    ++pos;
                    return;
                }
            }
        }
    }
}

static void skipEndOfLine(ReadBuffer & in, bool allow_cr_end_of_line)
{
    /// \n (Unix) or \r\n (DOS/Windows) or \n\r (Mac OS Classic)

    if (*in.position() == '\n')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\r')
            ++in.position();
    }
    else if (*in.position() == '\r')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\n')
            ++in.position();
        else if (!allow_cr_end_of_line)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot parse CSV2 format: found \\r (CR) not followed by \\n (LF)."
                " Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.");
    }
    else if (!in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected end of line");
}

/// Skip `whitespace` symbols allowed in CSV2.
static inline void skipWhitespacesAndTabs(ReadBuffer & in, const bool & allow_whitespace_or_tab_as_delimiter)
{
    if (allow_whitespace_or_tab_as_delimiter)
    {
        return;
    }
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}

CSV2FormatReader::CSV2FormatReader(PeekableReadBuffer & buf_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesReader(buf_, format_settings_), buf(&buf_)
{
}

void CSV2FormatReader::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
    assertChar(format_settings.csv2.delimiter, *buf);
}

template <bool read_string>
String CSV2FormatReader::readCSV2FieldIntoString()
{
    if (format_settings.csv2.trim_whitespaces) [[likely]]
        skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);

    String field;
    if constexpr (read_string)
        readCSV2String(field, *buf, format_settings.csv2);
    else
        readCSV2Field(field, *buf, format_settings.csv2);
    return field;
}

void CSV2FormatReader::skipField()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
    NullOutput out;
    readCSV2StringInto(out, *buf, format_settings.csv2);
}

void CSV2FormatReader::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);

    if (buf->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv2.delimiter)
        ++buf->position();

    skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
    if (buf->eof())
        return;

    skipEndOfLine(*buf, format_settings.csv2.allow_cr_end_of_line);
}

void CSV2FormatReader::skipHeaderRow()
{
    do
    {
        skipField();
        skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
    } while (checkChar(format_settings.csv2.delimiter, *buf));

    skipRowEndDelimiter();
}

template <bool is_header>
std::vector<String> CSV2FormatReader::readRowImpl()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readCSV2FieldIntoString<is_header>());
        skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
    } while (checkChar(format_settings.csv2.delimiter, *buf));

    skipRowEndDelimiter();
    return fields;
}

bool CSV2FormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    const char delimiter = format_settings.csv2.delimiter;

    try
    {
        skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
        assertChar(delimiter, *buf);
    }
    catch (const DB::Exception &)
    {
        if (*buf->position() == '\n' || *buf->position() == '\r')
        {
            out << "ERROR: Line feed found where delimiter (" << delimiter
                << ") is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has unescaped quotes in values.\n";
        }
        else
        {
            out << "ERROR: There is no delimiter (" << delimiter << "). ";
            verbosePrintString(buf->position(), buf->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool CSV2FormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);

    if (buf->eof())
        return true;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv2.delimiter)
    {
        ++buf->position();
        skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
        if (buf->eof())
            return true;
    }

    if (!buf->eof() && *buf->position() != '\n' && *buf->position() != '\r')
    {
        out << "ERROR: There is no line feed. ";
        verbosePrintString(buf->position(), buf->position() + 1, out);
        out << " found instead.\n"
               " It's like your file has more columns than expected.\n"
               "And if your file has the right number of columns, maybe it has an unquoted string value with a comma.\n";

        return false;
    }

    skipEndOfLine(*buf, format_settings.csv2.allow_cr_end_of_line);
    return true;
}

bool CSV2FormatReader::allowVariableNumberOfColumns() const
{
    return format_settings.csv2.allow_variable_number_of_columns;
}

bool CSV2FormatReader::readField(
    IColumn & column,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    bool is_last_file_column,
    const String & /*column_name*/)
{
    if (format_settings.csv2.trim_whitespaces || !isStringOrFixedString(removeNullable(type))) [[likely]]
        skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);

    const bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv2.delimiter;
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || *buf->position() == '\r');

    /// Note: Tuples are serialized in CSV2 as separate columns, but with empty_as_default or null_as_default
    /// only one empty or NULL column will be expected
    if (format_settings.csv2.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        /// Treat empty unquoted column value as default value, if
        /// specified in the settings. Tuple columns might seem
        /// problematic, because they are never quoted but still contain
        /// commas, which might be also used as delimiters. However,
        /// they do not contain empty unquoted fields, so this check
        /// works for tuples as well.
        column.insertDefault();
        return false;
    }

    if (format_settings.csv2.use_default_on_bad_values)
        return readFieldOrDefault(column, type, serialization);
    return readFieldImpl(*buf, column, type, serialization);
}

bool CSV2FormatReader::readFieldImpl(
    ReadBuffer & istr, DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization)
{
    if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
    {
        /// If value is null but type is not nullable then use default value instead.
        return SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV2(column, istr, format_settings, serialization);
    }

    /// Read the column normally.
    serialization->deserializeTextCSV2(column, istr, format_settings);
    return true;
}

bool CSV2FormatReader::readFieldOrDefault(DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization)
{
    String field;
    readCSV2Field(field, *buf, format_settings.csv2);
    ReadBufferFromString tmp_buf(field);
    bool is_bad_value = false;
    bool res = false;

    size_t col_size = column.size();
    try
    {
        res = readFieldImpl(tmp_buf, column, type, serialization);
        /// Check if we parsed the whole field successfully.
        if (!field.empty() && !tmp_buf.eof())
            is_bad_value = true;
    }
    catch (const Exception &)
    {
        is_bad_value = true;
    }

    if (!is_bad_value)
        return res;

    if (column.size() == col_size + 1)
        column.popBack(1);
    column.insertDefault();
    return false;
}

void CSV2FormatReader::skipPrefixBeforeHeader()
{
    for (size_t i = 0; i != format_settings.csv2.skip_first_lines; ++i)
        readRow();
}

void CSV2FormatReader::setReadBuffer(ReadBuffer & in_)
{
    buf = assert_cast<PeekableReadBuffer *>(&in_);
    FormatWithNamesAndTypesReader::setReadBuffer(*buf);
}

bool CSV2FormatReader::checkForSuffix()
{
    if (!format_settings.csv2.skip_trailing_empty_lines)
        return buf->eof();

    PeekableReadBufferCheckpoint checkpoint(*buf);
    while (checkChar('\n', *buf) || checkChar('\r', *buf))
        ;
    if (buf->eof())
        return true;

    buf->rollbackToCheckpoint();
    return false;
}

bool CSV2FormatReader::checkForEndOfRow()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv2.allow_whitespace_or_tab_as_delimiter);
    return buf->eof() || *buf->position() == '\n' || *buf->position() == '\r';
}

CSV2SchemaReader::CSV2SchemaReader(ReadBuffer & in_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(
          buf,
          format_settings_,
          with_names_,
          with_types_,
          &reader,
          getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::CSV2),
          format_settings_.csv2.try_detect_header)
    , buf(in_)
    , reader(buf, format_settings_)
{
}

std::optional<std::pair<std::vector<String>, DataTypes>> CSV2SchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (buf.eof())
        return {};

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, format_settings, FormatSettings::EscapingRule::CSV2);
    return std::make_pair(std::move(fields), std::move(data_types));
}

std::optional<DataTypes> CSV2SchemaReader::readRowAndGetDataTypesImpl()
{
    auto fields_with_types = readRowAndGetFieldsAndDataTypes();
    if (!fields_with_types)
        return {};
    return std::move(fields_with_types->second);
}


void registerInputFormatCSV2(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerInputFormat(
            format_name,
            [with_names,
             with_types](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
            { return std::make_shared<CSV2RowInputFormat>(sample, buf, std::move(params), with_names, with_types, settings); });
    };

    registerWithNamesAndTypes("CSV2", register_func);
}

std::pair<bool, size_t> fileSegmentationEngineCSV2Impl(
    ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows, const FormatSettings & settings)
{
    char * pos = in.position();
    bool quotes = false;
    bool need_more_data = true;
    size_t number_of_rows = 0;

    if (max_rows && (max_rows < min_rows))
        max_rows = min_rows;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        if (quotes)
        {
            pos = find_first_symbols<'"'>(pos, in.buffer().end());
            if (pos > in.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            else if (pos == in.buffer().end())
                continue;
            else if (*pos == '"')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '"')
                    ++pos;
                else
                    quotes = false;
            }
        }
        else
        {
            pos = find_first_symbols<'"', '\r', '\n'>(pos, in.buffer().end());
            if (pos > in.buffer().end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
            else if (pos == in.buffer().end())
                continue;

            if (*pos == '"')
            {
                quotes = true;
                ++pos;
                continue;
            }

            if (*pos == '\n')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos) && *pos == '\r')
                    ++pos;
            }
            else if (*pos == '\r')
            {
                ++pos;
                if (settings.csv2.allow_cr_end_of_line)
                    continue;
                else if (loadAtPosition(in, memory, pos) && *pos == '\n')
                    ++pos;
                else
                    continue;
            }

            ++number_of_rows;
            if ((number_of_rows >= min_rows)
                && ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows)))
                need_more_data = false;
        }
    }

    saveUpToPosition(in, memory, pos);
    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineCSV2(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool, bool)
    {
        static constexpr size_t min_rows = 3; /// Make it 3 for header auto detection (first 3 rows must be always in the same segment).
        factory.registerFileSegmentationEngineCreator(
            format_name,
            [](const FormatSettings & settings) -> FormatFactory::FileSegmentationEngine
            {
                return [settings](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
                { return fileSegmentationEngineCSV2Impl(in, memory, min_bytes, min_rows, max_rows, settings); };
            });
    };

    registerWithNamesAndTypes("CSV2", register_func);
    markFormatWithNamesAndTypesSupportsSamplingColumns("CSV2", factory);
}

void registerCSV2SchemaReader(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerSchemaReader(
            format_name,
            [with_names, with_types](ReadBuffer & buf, const FormatSettings & settings)
            { return std::make_shared<CSV2SchemaReader>(buf, with_names, with_types, settings); });
        if (!with_types)
        {
            factory.registerAdditionalInfoForSchemaCacheGetter(
                format_name,
                [with_names](const FormatSettings & settings)
                {
                    String result = getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::CSV2);
                    if (!with_names)
                        result += fmt::format(
                            ", column_names_for_schema_inference={}, try_detect_header={}, skip_first_lines={}",
                            settings.column_names_for_schema_inference,
                            settings.csv2.try_detect_header,
                            settings.csv2.skip_first_lines);
                    return result;
                });
        }
    };

    registerWithNamesAndTypes("CSV2", register_func);
}

}
