#include <Parsers/ASTQualifiedAsterisk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTQualifiedAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    qualifier->appendColumnName(ostr);
    writeCString(".*", ostr);
}

void ASTQualifiedAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    qualifier->formatImpl(settings, state, frame);
    settings.ostr << ".*";

    /// Format column transformers
    for (const auto & child : transformers->children)
    {
        settings.ostr << ' ';
        child->formatImpl(settings, state, frame);
    }
}

}
