#include <cstdlib>
#include <getopt.h>
#include <libpq-fe.h>
#include <sstream>
#include <iostream>
#include <iomanip>

#define OSMIUM_MAIN
#include <osmium.hpp>

class pgCopyHandler : public Osmium::Handler::Base {

PGconn *conn;
static const char d = ';';
long long int count;

public:

    pgCopyHandler(PGconn *psql) : conn(psql) {
        count = 0;
    }

    ~pgCopyHandler() {
    }

    std::string tagsToHstore(Osmium::OSM::Node *node) {
        if(node->tag_count() != 0) {
            std::ostringstream out;
            out << "hstore(ARRAY[" << "'" << node->get_tag_key(0) << "','" << node->get_tag_value(0) << "'";
            for(int i=1;i<node->tag_count();++i) {
                out << ",'" << node->get_tag_key(i) << "','" << node->get_tag_value(i) << "'";
            }
            out << "])";
            std::string str(out.str());
            return str;
        } else {
            std::string str("");
            return str;
        }
    }

    void callback_init() {
        //std::cout << "BEGIN;" << std::endl;
    }

    void callback_node(Osmium::OSM::Node *node) {
        std::cout << node->get_id() << d << node->get_version() << d << node->get_uid() << d;
        std::cout << node->get_timestamp_as_string().replace(10,1," ").erase(19,1) << d;
        std::cout << node->get_changeset() << d << tagsToHstore(node) << d;
        std::cout << "SRID=4326\\;POINT(" << node->get_lat() << " " << node->get_lon() << ")";
        std::cout << std::endl;
        count++;
        if (count % 10000 == 0) {
            std::cerr << '\r';
            std::cerr << std::setfill(' ') << std::setw(19) << count;
            
            //std::cout << "END;" << std::endl;
            exit(0);
        }
    }

    void callback_way(Osmium::OSM::Way *way) {
        std::cerr << "Way!" << std::endl;
    }

    void callback_relation(Osmium::OSM::Relation *relation) {
        std::cerr << "Relation!" << std::endl;
    }

    void callback_final() {
        std::cerr << "END;" << std::endl;
    }

};

void print_help() {
    std::cout << "whosh [OPTIONS] [INFILE]\n\n" \
              << "If INFILE is not given stdin/stdout is assumed.\n" \
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

    std::string input;
    int remaining_args = argc - optind;
    if ( (remaining_args != 1) || host.empty() || db.empty() || user.empty() ) {
        std::cerr << "You are doin' it wrong. Use -h to get help." << std::endl;
        exit(1);
    } else { 
        input = argv[optind];
    }

    

    Osmium::Framework osmium(debug);

    Osmium::OSMFile infile(input);

    std::string conninfo = "dbname = " + db + " host = " + host + " user = "+ user + " password = " + pass;

    PGconn *conn;
    conn = PQconnectdb(conninfo.c_str());
    if(PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "OH NOES Thomeszing iz wron wit teh db conf oa teh internez" << std::endl;
        std::cerr << PQerrorMessage(conn) << std::endl;
        exit(1);
    }

    pgCopyHandler handler(conn);
    infile.read(handler);
}

