#include <argparse/argparse.hpp>
#include <iostream>
#include <stdexcept>

#include "csv_columnar.h"
#include "error.h"
#include "schema.h"

static void EnsureParentDirectory(const std::filesystem::path& path) {
    const auto parent = path.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
}

void ConfigureInferSchemaCommand(argparse::ArgumentParser& command) {
    command.add_description("Infer a schema from a CSV file.");
    command.add_argument("--input").required();
    command.add_argument("--output").required();
}

void ConfigureConvertCommand(argparse::ArgumentParser& command) {
    command.add_description("Convert CSV to the internal columnar format.");
    command.add_argument("--schema").required();
    command.add_argument("--input").required();
    command.add_argument("--output").required();
    command.add_argument("--row-group-size").scan<'u', size_t>().default_value(50000);
}

void ConfigureRoundtripCommand(argparse::ArgumentParser& command) {
    command.add_description("Convert a columnar file back to schema CSV and data CSV.");
    command.add_argument("--input").required();
    command.add_argument("--schema-output").required();
    command.add_argument("--csv-output").required();
}

int RunInferSchema(const argparse::ArgumentParser& command) {
    const auto input_path = std::filesystem::path(command.get<std::string>("--input"));
    const auto output_path = std::filesystem::path(command.get<std::string>("--output"));
    EnsureParentDirectory(output_path);
    WriteSchemaCsv(output_path, InferSchemaCsv(input_path));
    return 0;
}

int RunConvert(const argparse::ArgumentParser& command) {
    const auto output_path = std::filesystem::path(command.get<std::string>("--output"));
    EnsureParentDirectory(output_path);
    ConvertCsvToColumnar(std::filesystem::path(command.get<std::string>("--schema")),
                         std::filesystem::path(command.get<std::string>("--input")), output_path,
                         command.get<size_t>("--row-group-size"));
    return 0;
}

int RunRoundtrip(const argparse::ArgumentParser& command) {
    const auto schema_output_path = std::filesystem::path(command.get<std::string>("--schema-output"));
    const auto csv_output_path = std::filesystem::path(command.get<std::string>("--csv-output"));
    EnsureParentDirectory(schema_output_path);
    EnsureParentDirectory(csv_output_path);
    ConvertColumnarToCsv(std::filesystem::path(command.get<std::string>("--input")), schema_output_path,
                         csv_output_path);
    return 0;
}

int main(const int argc, char** argv) {
    try {
        argparse::ArgumentParser program("columnar_engine");
        argparse::ArgumentParser infer_schema_command("infer-schema");
        argparse::ArgumentParser convert_command("convert");
        argparse::ArgumentParser roundtrip_command("roundtrip");

        ConfigureInferSchemaCommand(infer_schema_command);
        ConfigureConvertCommand(convert_command);
        ConfigureRoundtripCommand(roundtrip_command);

        program.add_subparser(infer_schema_command);
        program.add_subparser(convert_command);
        program.add_subparser(roundtrip_command);
        program.parse_args(argc, argv);

        if (program.is_subcommand_used("infer-schema")) {
            return RunInferSchema(infer_schema_command);
        }
        if (program.is_subcommand_used("convert")) {
            return RunConvert(convert_command);
        }
        if (program.is_subcommand_used("roundtrip")) {
            return RunRoundtrip(roundtrip_command);
        }

        throw Error::InvalidArgument("app", "a subcommand is required");
    } catch (const Error& ex) {
        std::cerr << ex.what() << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    return 1;
}
