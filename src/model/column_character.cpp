#include "column_character.h"

#include "parsing.h"

void CharacterColumn::AppendFromString(const std::string& value) { AppendValue(ParseCharacter(value)); }

std::string CharacterColumn::ValueAsString(const size_t row) const { return std::string(1, ValueAt(row)); }
