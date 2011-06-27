#include <cstdlib>
#include <getopt.h>
#include <libpq-fe.h>
#include <sstream>
#include <iostream>
#include <iomanip>

#define OSMIUM_MAIN
#include <osmium.hpp>

class pgCopyHandler : public Osmium::Handler::Base {

PGconn *node_conn, *way_conn, *rel_conn;
static const char d = ';';
long long int node_count;
long long int way_count;
long long int rel_count;

public:

    pgCopyHandler(std::string dbConnectionString) {
        node_count = 0;
        way_count = 0;
        rel_count = 0;
        
/*        node_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(node_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Nodes failed: ";
            std::cerr << PQerrorMessage(node_conn) << std::endl;
            exit(1);
        }
        
        way_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(way_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Ways failed: ";
            std::cerr << PQerrorMessage(way_conn) << std::endl;
            exit(1);
        }
        
        rel_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(rel_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Relations failed: ";
            std::cerr << PQerrorMessage(rel_conn) << std::endl;
            exit(1);
        }
*/
    }

    ~pgCopyHandler() {
    }

    std::string escape(std::string str) {
        std::ostringstream ret;
        size_t i;
        for(i=0;i<str.length();++i) {
            if (str[i] == d) {
                ret << "\\";
            }
            if (str[i] == '\"') {
                ret << "\\\\";
            }
            ret << str[i];
        }
        return ret.str();
    }

    std::string tagsToHstore(Osmium::OSM::Object *obj) {
        if(obj->tag_count() != 0) {
            std::ostringstream out;
            out << "\"" << escape(obj->get_tag_key(0)) << "\"=>\"" << escape(obj->get_tag_value(0)) << "\"";
            for(int i=1;i<obj->tag_count();++i) {
                out << ",\"" << escape(obj->get_tag_key(i)) << "\"=>\"" << escape(obj->get_tag_value(i)) << "\"";
            }
            return out.str();
        } else {
            std::string str("");
            return str;
        }
    }

    std::string genNodesArray(Osmium::OSM::Way *way) {
        osm_sequence_id_t nodecount = way->node_count();
        osm_sequence_id_t i;
        std::ostringstream out;
        if (nodecount > 0) {
            out << "{" << way->get_node_id(0);
            for(i=1;i<nodecount;++i) {
                out << "," << way->get_node_id(i);
            }
            out << "}";
        }
        else {
            out << "{}";
        }
        return out.str();
    }

    void callback_init() {
        std::cerr << "Sending BEGIN" << std::endl;
        //std::cout << "BEGIN;" << std::endl;
    }

    void callback_node(Osmium::OSM::Node *node) {
        std::ostringstream node_str;
        node_str << node->get_id() << d;
        node_str << node->get_version() << d;
        node_str << node->get_uid() << d;
        node_str << node->get_timestamp_as_string().replace(10,1," ").erase(19,1) << d;
        node_str << node->get_changeset() << d;
        node_str << tagsToHstore(node) << d;
        node_str << "SRID=4326\\;POINT(" << node->get_lat() << " " << node->get_lon() << ")";
        node_str << std::endl;
        std::cout << node_str.str();

        node_count++;
        if (node_count % 10000 == 0) {
            std::cerr << '\r';
            std::cerr << "Nodes: " << node_count << " Ways: " << way_count << " Relations: " << rel_count;
        }
    }

    void callback_way(Osmium::OSM::Way *way) {
        std::ostringstream way_str;
        way_str << way->get_id() << d;
        way_str << way->get_version() << d;
        way_str << way->get_uid() << d;
        way_str << way->get_timestamp_as_string().replace(10,1," ").erase(19,1) << d;
        way_str << way->get_changeset() << d;
        way_str << tagsToHstore(way) << d;
        way_str << d;
        way_str << d;
        way_str << std::endl;
        //std::cout << way_str.str();

        way_count++;
        if (way_count % 10000 == 0) {
            std::cerr << '\r';
            std::cerr << "Nodes: " << node_count << " Ways: " << way_count << " Relations: " << rel_count;
        }
    }

    void callback_relation(Osmium::OSM::Relation *relation) {
        rel_count++;
        if (rel_count % 10000 == 0) {
            std::cerr << '\r';
            std::cerr << "Nodes: " << node_count << " Ways: " << way_count << " Relations: " << rel_count;
        }
    }

    void callback_final() {
        std::cerr << "END;" << std::endl;
    }

};

void print_help() {
    std::cout << "whosh [OPTIONS] [INFILE]\n\n" \
              << "If INFILE is not given stdin is assumed.\n" \
              << "File format is given as suffix in format .TYPE[.ENCODING].\n" \
              << "\nFile types:\n" \
              << "  osm     normal OSM file\n" \
              << "  osh     OSM file with history information\n" \
              << "\nFile encodings:\n" \
              << "  (none)  XML encoding\n" \
              << "  gz      XML encoding compressed with gzip\n" \
              << "  bz2     XML encoding compressed with bzip2\n" \
              << "  pbf     binary PBF encoding\n" \
              << "\nOptions:\n" \
              << "  -h, --help                This help message\n" \
              << "  -d, --debug               Enable debugging output\n" \
              << "  -c, --host=<dbhost>       DB Host\n" \
              << "  -p, --port=<dbport>       DB port\n" \
              << "  -b, --db=<db>             Database\n" \
              << "  -u, --user=<dbuser>       DB User\n" \
              << "  -w, --pass=<dbpass>       DB Password\n";
}

int main(int argc, char *argv[]) {
    static struct option long_options[] = {
        {"debug",       no_argument, 0, 'd'},
        {"help",        no_argument, 0, 'h'},
        {"host",  required_argument, 0, 'c'},
        {"port",  required_argument, 0, 'p'},
        {"db",    required_argument, 0, 'b'},
        {"user",  required_argument, 0, 'u'},
        {"pass",  required_argument, 0, 'w'},
        {0, 0, 0, 0}
    };

    bool debug = false;

    std::string host="";
    std::string port="";
    std::string db="";
    std::string user="";
    std::string pass="";

    while (true) {
        int c = getopt_long(argc, argv, "dhc:p:b:u:w:", long_options, 0);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'd':
                debug = true;
                break;
            case 'h':
                print_help();
                exit(0);
            case 'c':
                host = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'b':
                db = optarg;
                break;
            case 'u':
                user = optarg;
                break;
            case 'w':
                pass = optarg;
                break;
            default:
                exit(1);
        }
    }

    int remaining_args = argc - optind;
    if ( (remaining_args != 1) || host.empty() || db.empty() || user.empty() ) {
        std::cerr << "You are doin' it wrong. Use -h to get help." << std::endl;
        exit(1);
    }

    std::string input = argv[optind]; 
    std::string conninfo = "dbname = " + db + " host = " + host + " user = "+ user + " password = " + pass;

    Osmium::Framework osmium(debug);
    Osmium::OSMFile infile(input);

    pgCopyHandler handler(conninfo);
    infile.read(handler);
}

