// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <string_view>

#include <errno.h>
#include <fcntl.h>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/algorithm/string.hpp>

#include "common/debug.h"

#include "MDSAuthCaps.h"

#define dout_subsys ceph_subsys_mds

#undef dout_prefix
#define dout_prefix *_dout << "MDSAuthCap "

using std::ostream;
using std::string;
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace phoenix = boost::phoenix;

template <typename Iterator>
struct MDSCapParser : qi::grammar<Iterator, MDSAuthCaps()>
{
  MDSCapParser() : MDSCapParser::base_type(mdscaps)
  {
    using qi::char_;
    using qi::int_;
    using qi::uint_;
    using qi::lexeme;
    using qi::alnum;
    using qi::_val;
    using qi::_1;
    using qi::_2;
    using qi::_3;
    using qi::eps;
    using qi::lit;

    spaces = +(lit(' ') | lit('\n') | lit('\t'));

    quoted_path %=
      lexeme[lit("\"") >> *(char_ - '"') >> '"'] | 
      lexeme[lit("'") >> *(char_ - '\'') >> '\''];
    unquoted_path %= +char_("a-zA-Z0-9_./-");

    // match := [path=<path>] [uid=<uid> [gids=<gid>[,<gid>...]]
    path %= (spaces >> lit("path") >> lit('=') >> (quoted_path | unquoted_path));
    uid %= (spaces >> lit("uid") >> lit('=') >> uint_);
    uintlist %= (uint_ % lit(','));
    gidlist %= -(spaces >> lit("gids") >> lit('=') >> uintlist);
    match = -(
	     (uid >> gidlist)[_val = phoenix::construct<MDSCapMatch>(_1, _2)] |
	     (path >> uid >> gidlist)[_val = phoenix::construct<MDSCapMatch>(_1, _2, _3)] |
             (path)[_val = phoenix::construct<MDSCapMatch>(_1)]);

    // capspec = * | r[w][p][s]
    capspec = spaces >> (
        lit("*")[_val = MDSCapSpec(MDSCapSpec::ALL)]
        |
        lit("all")[_val = MDSCapSpec(MDSCapSpec::ALL)]
        |
        (lit("rwps"))[_val = MDSCapSpec(MDSCapSpec::RWPS)]
        |
        (lit("rwp"))[_val = MDSCapSpec(MDSCapSpec::RWP)]
        |
        (lit("rws"))[_val = MDSCapSpec(MDSCapSpec::RWS)]
        |
        (lit("rw"))[_val = MDSCapSpec(MDSCapSpec::RW)]
        |
        (lit("r"))[_val = MDSCapSpec(MDSCapSpec::READ)]
        );
    idmap = *(spaces >> lit("idmap"));

    grant = lit("allow") >> (capspec >> match)[_val = phoenix::construct<MDSCapGrant>(_1, _2)];
    grants %= (grant % (*lit(' ') >> (lit(';') | lit(',')) >> *lit(' ')));
    mdscaps = (grants >> idmap) [_val = phoenix::construct<MDSAuthCaps>(_1, _2)]; 
  }
  qi::rule<Iterator> spaces;
  qi::rule<Iterator, string()> quoted_path, unquoted_path;
  qi::rule<Iterator, MDSCapSpec()> capspec;
  qi::rule<Iterator, string()> path;
  qi::rule<Iterator, uint32_t()> uid;
  qi::rule<Iterator, std::vector<uint32_t>() > uintlist;
  qi::rule<Iterator, std::vector<uint32_t>() > gidlist;
  qi::rule<Iterator, std::string() > idmap;
  qi::rule<Iterator, MDSCapMatch()> match;
  qi::rule<Iterator, MDSCapGrant()> grant;
  qi::rule<Iterator, std::vector<MDSCapGrant>()> grants;
  qi::rule<Iterator, MDSAuthCaps()> mdscaps;
};

void MDSCapMatch::normalize_path()
{
  // drop any leading /
  while (path.length() && path[0] == '/') {
    path = path.substr(1);
  }

  // drop dup //
  // drop .
  // drop ..
}

bool MDSCapMatch::match(std::string_view target_path,
			const int caller_uid,
			const int caller_gid,
			const vector<uint64_t> *caller_gid_list) const
{
  if (uid != MDS_AUTH_UID_ANY) {
    if (uid != caller_uid)
      return false;
    if (!gids.empty()) {
      bool gid_matched = false;
      if (std::find(gids.begin(), gids.end(), caller_gid) != gids.end())
	gid_matched = true;
      if (caller_gid_list) {
	for (auto i = caller_gid_list->begin(); i != caller_gid_list->end(); ++i) {
	  if (std::find(gids.begin(), gids.end(), *i) != gids.end()) {
	    gid_matched = true;
	    break;
	  }
	}
      }
      if (!gid_matched)
	return false;
    }
  }

  if (!match_path(target_path)) {
    return false;
  }

  return true;
}

bool MDSCapMatch::match_path(std::string_view target_path) const
{
  if (path.length()) {
    if (target_path.find(path) != 0)
      return false;
    // if path doesn't already have a trailing /, make sure the target
    // does so that path=/foo doesn't match target_path=/food
    if (target_path.length() > path.length() &&
	path[path.length()-1] != '/' &&
	target_path[path.length()] != '/')
      return false;
  }

  return true;
}

/**
 * Is the client *potentially* able to access this path?  Actual
 * permission will depend on uids/modes in the full is_capable.
 */
bool MDSAuthCaps::path_capable(std::string_view inode_path) const
{
  for (const auto &i : grants) {
    if (i.match.match_path(inode_path)) {
      return true;
    }
  }

  return false;
}

/**
 * For a given filesystem path, query whether this capability carries`
 * authorization to read or write.
 *
 * This is true if any of the 'grant' clauses in the capability match the
 * requested path + op.
 */
bool MDSAuthCaps::is_capable(std::string_view inode_path,
			     uid_t inode_uid, gid_t inode_gid,
			     unsigned inode_mode,
			     uid_t caller_uid, gid_t caller_gid,
			     const vector<uint64_t> *caller_gid_list,
			     unsigned mask,
			     uid_t new_uid, gid_t new_gid) const
{
  if (cct)
    ldout(cct, 10) << __func__ << " inode(path /" << inode_path
		   << " owner " << inode_uid << ":" << inode_gid
		   << " mode 0" << std::oct << inode_mode << std::dec
		   << ") by caller " << caller_uid << ":" << caller_gid
                // << "[" << caller_gid_list << "]";
		   << " mask " << mask
		   << " new " << new_uid << ":" << new_gid
		   << " cap: " << *this << dendl;

  for (std::vector<MDSCapGrant>::const_iterator i = grants.begin();
       i != grants.end();
       ++i) {

    if (i->match.match(inode_path, caller_uid, caller_gid, caller_gid_list) &&
	i->spec.allows(mask & (MAY_READ|MAY_EXECUTE), mask & MAY_WRITE)) {
      // we have a match; narrow down GIDs to those specifically allowed here
      vector<uint64_t> gids;
      if (std::find(i->match.gids.begin(), i->match.gids.end(), caller_gid) !=
	  i->match.gids.end()) {
	gids.push_back(caller_gid);
      }

      if (caller_gid_list) {
	std::set_intersection(i->match.gids.begin(), i->match.gids.end(),
			      caller_gid_list->begin(), caller_gid_list->end(),
			      std::back_inserter(gids));
	std::sort(gids.begin(), gids.end());
      }
      

      // Spec is non-allowing if caller asked for set pool but spec forbids it
      if (mask & MAY_SET_VXATTR) {
        if (!i->spec.allow_set_vxattr()) {
          continue;
        }
      }

      if (mask & MAY_SNAPSHOT) {
        if (!i->spec.allow_snapshot()) {
          continue;
        }
      }

      // check unix permissions?
      if (i->match.uid == MDSCapMatch::MDS_AUTH_UID_ANY) {
        ldout(cct, 1) << __func__ << " i->match.uid = " << i->match.uid << dendl;
        return true;
      }

      // chown/chgrp
      if (mask & MAY_CHOWN) {
	if (new_uid != caller_uid ||   // you can't chown to someone else
	    inode_uid != caller_uid) { // you can't chown from someone else
	  continue;
	}
      }

      if (mask & MAY_CHGRP) {
	// you can only chgrp *to* one of your groups... if you own the file.
	if (inode_uid != caller_uid ||
	    std::find(gids.begin(), gids.end(), new_gid) ==
	    gids.end()) {
	  continue;
	}
      }

      if (inode_uid == caller_uid) {
        if ((!(mask & MAY_READ) || (inode_mode & S_IRUSR)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWUSR)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXUSR))) {
          return true;
        }
      } else if (std::find(gids.begin(), gids.end(),
			   inode_gid) != gids.end()) {
        if ((!(mask & MAY_READ) || (inode_mode & S_IRGRP)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWGRP)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXGRP))) {
          return true;
        }
      } else {
        if ((!(mask & MAY_READ) || (inode_mode & S_IROTH)) &&
	    (!(mask & MAY_WRITE) || (inode_mode & S_IWOTH)) &&
	    (!(mask & MAY_EXECUTE) || (inode_mode & S_IXOTH))) {
          return true;
        }
      }
    }
  }
  return false;
}

void MDSAuthCaps::set_allow_all()
{
    grants.clear();
    grants.push_back(MDSCapGrant(MDSCapSpec(MDSCapSpec::ALL), MDSCapMatch()));
}

bool MDSAuthCaps::idmap_required() 
{
    return idmap; 
}

bool MDSAuthCaps::parse(CephContext *c, std::string_view str, ostream *err)
{
  // Special case for legacy caps
  if (str == "allow") {
    grants.clear();
    grants.push_back(MDSCapGrant(MDSCapSpec(MDSCapSpec::RWPS), MDSCapMatch()));
    return true;
  }

  auto iter = str.begin();
  auto end = str.end();

  MDSCapParser<decltype(iter)> g;

  bool r = qi::phrase_parse(iter, end, g, ascii::space, *this);
  idmap = (str.find("idmap") != std::string::npos);

  cct = c;  // set after parser self-assignment

  if (r && iter == end) {
    for (auto& grant : grants) {
      std::sort(grant.match.gids.begin(), grant.match.gids.end());
    }
    return true;
  } else {
    // Make sure no grants are kept after parsing failed!
    grants.clear();

    if (err)
      *err << "MDSAuthCaps parse failed, stopped at '" << std::string(iter, end)
           << "' of '" << str << "'\n";
    return false; 
  }
}


bool MDSAuthCaps::allow_all() const
{
  for (std::vector<MDSCapGrant>::const_iterator i = grants.begin(); i != grants.end(); ++i) {
    if (i->match.is_match_all() && i->spec.allow_all()) {
      return true;
    }
  }

  return false;
}

vector<uint64_t> MDSAuthCaps::update_ids(const string& name, bool& is_valid) {

  vector<string> backends = get_idmap_backend();
  vector<uint64_t> ids;

  for (size_t i = 0; i < backends.size(); ++i) {
    if (backends[i] == "ldap") {
      ids = ldap_lookup(name, is_valid);

      if (!ids.empty()) {
        for (auto& grant : grants) {
          grant.match.uid = ids[0];
          grant.match.gids.clear();
          for (auto i = ids.begin() + 2; i != ids.end(); ++i) {
            grant.match.gids.push_back(*i); 
          }
        }
      } else { 
        ldout(cct, 1) << __func__ << " ldap lookup failure. Maintaining original ids." << dendl;
      }
    }

    if (backends[i] == "key") {
      ldout(cct, 1) << __func__ << " backend selected = key" << dendl;
    }

    if (!ids.empty()) {
      string gids_list = "[";
      for (size_t i = 2; i < ids.size() - 1; ++i) {
        gids_list += to_string(ids[i]);
        gids_list += ", ";
      }
      gids_list += to_string(ids.back());
      gids_list += "]";
 
      ldout(cct, 5) << __func__ << " set client uid to: " << ids[0] << dendl;
      ldout(cct, 5) << __func__ << " set client gid to: " << ids[1] << dendl;
      ldout(cct, 5) << __func__ << " set group gids list to: " << gids_list << dendl;
      return ids;
    }
  }
}

vector<string> MDSAuthCaps::get_idmap_backend()
{
  string backend = g_conf->get_val<string>("mds_idmap_backend");
  boost::erase_all(backend, " ");
  vector<string> backend_list; string item;
  stringstream str(backend);
  while (getline(str, item, ',')) {
    backend_list.push_back(item);
  }
  return backend_list;
}

string MDSAuthCaps::get_ldap_bindpw()
{
  string ldap_bindpw = "";
  string ldap_plaintext = g_conf->get_val<string>("mds_idmap_ldap_bindpw");
  string ldap_secret = g_conf->get_val<string>("mds_idmap_ldap_secret");

  if (!ldap_secret.empty()) {
    char bindpw[1024];
    memset(bindpw, 0, 1024);
    int pwlen = safe_read_file("" /* base */, ldap_secret.c_str(), bindpw, 1023);
    if (pwlen) {
      ldap_bindpw = bindpw;
      boost::algorithm::trim(ldap_bindpw);
      if (ldap_bindpw.back() == '\n') { ldap_bindpw.pop_back(); }
    }
  } 
  
  if (ldap_bindpw.empty() && !ldap_plaintext.empty()) {
    ldap_bindpw = ldap_plaintext;
    if (ldap_bindpw.back() == '\n') { ldap_bindpw.pop_back(); }
  }

  return ldap_bindpw;
}

vector<uint64_t> MDSAuthCaps::ldap_lookup(const string& name, bool& is_valid) {

  LDAP *ld;
  LDAPMessage *result, *e;
  BerElement *ber;
  int version = LDAP_VERSION3;
  int rc;

  vector<uint64_t> err;

  string ldap_uri = g_conf->get_val<string>("mds_idmap_ldap_uri");

  rc = ldap_initialize(&ld, ldap_uri.c_str());
  if (rc != LDAP_SUCCESS) {
    ldout(cct, 1) << __func__ << " ldap_initialize failed during client session with uri " << ldap_uri 
      << ". Ldap error: " << ldap_err2string(rc) << dendl;
    is_valid = false;
    return err;
  }    

  rc = ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &version);
  if (rc != LDAP_SUCCESS) {
    ldout(cct, 1) << __func__ << " ldap_set_option failed during client session with uri " << ldap_uri 
      << ". Ldap error: " << ldap_err2string(rc) << dendl;
    is_valid = false;
    return err;
  }    

  string ldap_bind_dn = g_conf->get_val<string>("mds_idmap_ldap_binddn");
  string ldap_bind_pw = get_ldap_bindpw();

  if (ldap_bind_dn.empty()) {
    rc = ldap_simple_bind_s(ld, NULL, NULL);
    if (rc != LDAP_SUCCESS) {
      ldout(cct, 1) << __func__ << " ldap_simple_bind_s failed during client session with uri " << ldap_uri 
        << ". Ldap error: " << ldap_err2string(rc) << dendl;
      is_valid = false;
      return err;
    }    
  } else {
    rc = ldap_simple_bind_s(ld, ldap_bind_dn.c_str(), ldap_bind_pw.c_str());
    if (rc != LDAP_SUCCESS) {
      ldout(cct, 1) << __func__ << " ldap_simple_bind_s failed during client session with uri " << ldap_uri 
        << ". Ldap error: " << ldap_err2string(rc) << dendl;
      is_valid = false;
      return err;
    }    
  }

  // Lookup for client uidNumber, gidNumber, and desired group search attribute
  vector<uint64_t> ids;
  uint64_t uidNumber, gidNumber;
  string base_dn = g_conf->get_val<string>("mds_idmap_ldap_basedn");

  string groupAttr = g_conf->get_val<string>("mds_idmap_ldap_groupattr");  
  
  char* attrs[] = { "uidNumber", "gidNumber", const_cast<char*>(groupAttr.c_str()), NULL };
  if (groupAttr == "dn") { attrs[2] = NULL; }

  string searchAttr = g_conf->get_val<string>("mds_idmap_ldap_idattr");
  string searchVal = name; 

  string filter_str = "(";
  filter_str += searchAttr;
  filter_str += "=";
  filter_str += searchVal;
  filter_str += ")";
  const char* filter = filter_str.c_str();

  rc = ldap_search_ext_s(ld, base_dn.c_str(), LDAP_SCOPE_SUBTREE, filter, attrs, 0, NULL, NULL, NULL, LDAP_NO_LIMIT, &result);
  ldout(cct, 5) << __func__ << " results of ldap search using query \""<< filter << "\": " << ldap_err2string(rc) << dendl;
  if (rc != LDAP_SUCCESS) {
    ldout(cct, 1) << __func__ << " ldap_search_ext_s failed during client session." 
      << " Did not find attribute " << searchAttr << " matching value " << searchVal
      << ". Ldap error: " << ldap_err2string(rc) << dendl;
    is_valid = false;
    return err;
  } 

  e = ldap_first_entry(ld, result);

  if (e != NULL) {
    char* uidNumAttr = ldap_first_attribute(ld, e, &ber);
    char* gidNumAttr = ldap_next_attribute(ld, e, ber);

    if (uidNumAttr == NULL || gidNumAttr == NULL) {
      ldout(cct, 1) << __func__ << " ldap search results error: No UID or GID Attribute found for client " 
                    << name << ". Lookup failed." << dendl;
      is_valid = false;
      return err;
    }

    char** uidNumVals = ldap_get_values(ld, e, uidNumAttr);
    char** gidNumVals = ldap_get_values(ld, e, gidNumAttr);

    if (uidNumVals == NULL || gidNumVals == NULL) {
      ldout(cct, 1) << __func__ << " ldap search results error: No UID or GID Values found for client " 
                    << name << ". Lookup failed." << dendl;
      is_valid = false;
      return err;
    }

    uidNumber = uint64_t(atoi(uidNumVals[0])); ids.push_back(uidNumber);
    gidNumber = uint64_t(atoi(gidNumVals[0])); ids.push_back(gidNumber);
    if (uidNumber == 0 || gidNumber == 0) { is_valid = false; return err;}

    if (groupAttr != "dn") {
      char* grpSearchAttr = ldap_next_attribute(ld, e, ber);
      char** grpSearchVals = ldap_get_values(ld, e, grpSearchAttr);
      searchVal = grpSearchVals[0];
      searchAttr = g_conf->get_val<string>("mds_idmap_ldap_memberattr");
      ldap_value_free(grpSearchVals);
    } else {
      searchVal = ldap_get_dn(ld, e);
      searchAttr = g_conf->get_val<string>("mds_idmap_ldap_memberattr");
    }

    // Free memory
    ldap_value_free(uidNumVals);
    ldap_value_free(gidNumVals);

    if (ber != NULL) {
      ber_free(ber, 0);
    }

    ldap_msgfree(result);
    result = NULL; ber = NULL;

    // Lookup for group GIDs
    base_dn = g_conf->get_val<string>("mds_idmap_ldap_groupdn");

    filter_str = "(";
    filter_str += searchAttr;
    filter_str += "=";
    filter_str += searchVal;
    filter_str += ")";
    filter = filter_str.c_str();

    char* gidAttrs[] = { "gidNumber", NULL };

    rc = ldap_search_ext_s(ld, base_dn.c_str(), LDAP_SCOPE_SUBTREE, filter, gidAttrs, 0, NULL, NULL, NULL, LDAP_NO_LIMIT, &result);
    ldout(cct, 5) << __func__ << " results of ldap search using query \""<< filter << "\": " << ldap_err2string(rc) << dendl;
    if (rc != LDAP_SUCCESS) {
      ldout(cct, 1) << __func__ << " ldap_search_ext_s failed during client session." 
        << " Did not find attribute " << searchAttr << " matching value " << searchVal
        << ". Ldap error: " << ldap_err2string(rc) << dendl;
      is_valid = false;
      return err;
    }

    if (e == NULL) {
      ldout(cct, 1) << __func__ << "ldap search results error: No groups' gids found for client " << name << "." << dendl;
      is_valid = false;
      return err;
    }

    for (e = ldap_first_entry( ld, result ); e != NULL; e = ldap_next_entry( ld, e )) {
      char* gidGrpAttr = ldap_first_attribute(ld, e, &ber);
      char** gidGrpVals = ldap_get_values(ld, e, gidGrpAttr);
      for (size_t i = 0; gidGrpVals[i] != NULL; ++i) {
        ids.push_back(uint64_t(atoi(gidGrpVals[i])));
      }
      ldap_value_free(gidGrpVals);
    }

    if (ber != NULL) {
      ber_free( ber, 0 );
    }

    ldap_msgfree(result);

    if (uidNumber == NULL || gidNumber == NULL || ids.size() <= 2) {
      ldout(cct, 1) << __func__ << " ldap search results error: one or more of uid, gid, or group gids not found " << dendl;
      is_valid = false;
      return err;
    }
  } else {
    ldout(cct, 1) << " ldap search results error: No LDAP entry found for client " << name << ". Lookup failed." << dendl;
    is_valid = false;
    return err;
  }

  string gids_list = "[";
  for (size_t i = 2; i < ids.size() - 1; ++i) {
    gids_list += to_string(ids[i]);
    gids_list += ", ";
  }
  gids_list += to_string(ids.back());
  gids_list += "]";
 
  ldout(cct, 5) << __func__ << " set client uid to: " << ids[0] << dendl;
  ldout(cct, 5) << __func__ << " set client gid to: " << ids[1] << dendl;
  ldout(cct, 5) << __func__ << " set group gids list to: " << gids_list << dendl;
  
  return ids;
}

ostream &operator<<(ostream &out, const MDSCapMatch &match)
{
  if (match.path.length()) {
    out << "path=\"/" << match.path << "\"";
    if (match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
      out << " ";
    }
  }
  if (match.uid != MDSCapMatch::MDS_AUTH_UID_ANY) {
    out << "uid=" << match.uid;
    if (!match.gids.empty()) {
      out << " gids=";
      for (std::vector<gid_t>::const_iterator p = match.gids.begin();
	   p != match.gids.end();
	   ++p) {
	if (p != match.gids.begin())
	  out << ',';
	out << *p;
      }
    }
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSCapSpec &spec)
{
  if (spec.allow_all()) {
    out << "*";
  } else {
    if (spec.allow_read()) {
      out << "r";
    }
    if (spec.allow_write()) {
      out << "w";
    }
    if (spec.allow_set_vxattr()) {
      out << "p";
    }
    if (spec.allow_snapshot()) {
      out << "s";
    }
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSCapGrant &grant)
{
  out << "allow ";
  out << grant.spec;
  if (!grant.match.is_match_all()) {
    out << " " << grant.match;
  }

  return out;
}


ostream &operator<<(ostream &out, const MDSAuthCaps &cap)
{
  out << "MDSAuthCaps[";
  for (size_t i = 0; i < cap.grants.size(); ++i) {
    out << cap.grants[i];
    if (i < cap.grants.size() - 1) {
      out << ", ";
    }
  }
  out << "]";

  return out;
}

