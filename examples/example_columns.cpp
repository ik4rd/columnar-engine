#include <exception>
#include <iostream>
#include <sstream>
#include <string>

#include "column_int64.h"
#include "column_string.h"

int main() {
    try {
        Int64Column ids;
        StringColumn names;

        ids.AppendFromString("1");
        ids.AppendFromString("2");
        names.AppendFromString("alpha");
        names.AppendFromString("be,ta");

        std::stringstream id_stream;
        ids.WriteTo(id_stream);
        const std::string id_bytes = id_stream.str();

        Int64Column ids_read;
        std::stringstream id_in(id_bytes);
        ids_read.ReadFrom(id_in, 2, id_bytes.size());

        std::stringstream name_stream;
        names.WriteTo(name_stream);
        const std::string name_bytes = name_stream.str();

        StringColumn names_read;
        std::stringstream name_in(name_bytes);
        names_read.ReadFrom(name_in, 2, name_bytes.size());

        for (size_t i = 0; i < 2; ++i) {
            std::cout << "row " << i << ": [" << ids_read.ValueAsString(i) << "] [" << names_read.ValueAsString(i)
                      << "]" << std::endl;
        }
    } catch (const std::exception& ex) {
        std::cerr << "columns example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
