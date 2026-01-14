#include "magic.h"

std::string_view ColumnarMagic() { return "CLMN"; }
uint64_t FooterSize() { return sizeof(uint64_t) + ColumnarMagic().size(); }
