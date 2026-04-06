#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"

ABSL_FLAG(std::string, input, "", "Path to input file");

int main(const int argc, char** argv) {
    absl::SetProgramUsageMessage(
        "Usage:\n"
        "  converter [--input=PATH]");
    absl::ParseCommandLine(argc, argv);

    const auto input = absl::GetFlag(FLAGS_input);

    if (input.empty()) {
        std::cerr << absl::ProgramUsageMessage();
        return 1;
    }

    std::cout << input << std::endl;

    return 0;
}
