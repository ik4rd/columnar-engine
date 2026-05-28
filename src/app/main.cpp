#include <argparse/argparse.hpp>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "common/error.h"
#include "convert/csv_columnar.h"
#include "executor/executor.h"
#include "io/csv_batch.h"
#include "io/file.h"
#include "model/schema.h"
#include "model/schema_csv.h"

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
    command.add_argument("--row-group-size").scan<'u', size_t>().default_value(size_t{1 << 14});
    command.add_argument("--compression").default_value(std::string("none"));
}

void ConfigureRoundtripCommand(argparse::ArgumentParser& command) {
    command.add_description("Convert a columnar file back to schema CSV and data CSV.");
    command.add_argument("--input").required();
    command.add_argument("--schema-output").required();
    command.add_argument("--csv-output").required();
}

void ConfigureRunQueryCommand(argparse::ArgumentParser& command) {
    command.add_description("Execute a SQL query against a columnar file and write the result to CSV.");
    command.add_argument("--input").required();
    command.add_argument("--output").required();
    command.add_argument("--table-name").default_value(std::string("hits"));
    command.add_argument("--query");
    command.add_argument("--query-file");
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
                         command.get<size_t>("--row-group-size"),
                         CompressionFromName(command.get<std::string>("--compression")));

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

int RunQuery(const argparse::ArgumentParser& command) {
    const bool has_query = command.is_used("--query");
    const bool has_query_file = command.is_used("--query-file");

    if (has_query == has_query_file) {
        throw Error::InvalidArgument("app", "exactly one of --query or --query-file must be specified");
    }

    const std::string query = has_query ? command.get<std::string>("--query")
                                        : ReadTextFile(std::filesystem::path(command.get<std::string>("--query-file")));

    const auto output_path = std::filesystem::path(command.get<std::string>("--output"));
    EnsureParentDirectory(output_path);

    Executor executor;
    executor.RegisterTable(command.get<std::string>("--table-name"),
                           std::filesystem::path(command.get<std::string>("--input")));

    const ExecuteExpected result = executor.Execute(query);
    if (!result.has_value()) {
        throw result.error();
    }

    WriteBatchCsv(output_path, result.value());

    return 0;
}

int main(const int argc, char** argv) {
    try {
        argparse::ArgumentParser program("columnar_engine");
        argparse::ArgumentParser infer_schema_command("infer-schema");
        argparse::ArgumentParser convert_command("convert");
        argparse::ArgumentParser roundtrip_command("roundtrip");
        argparse::ArgumentParser run_query_command("run-query");

        ConfigureInferSchemaCommand(infer_schema_command);
        ConfigureConvertCommand(convert_command);
        ConfigureRoundtripCommand(roundtrip_command);
        ConfigureRunQueryCommand(run_query_command);

        program.add_subparser(infer_schema_command);
        program.add_subparser(convert_command);
        program.add_subparser(roundtrip_command);
        program.add_subparser(run_query_command);
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

        if (program.is_subcommand_used("run-query")) {
            return RunQuery(run_query_command);
        }

        throw Error::InvalidArgument("app", "a subcommand is required");
    } catch (const Error& ex) {
        std::cerr << ex.what() << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    return 1;
}
