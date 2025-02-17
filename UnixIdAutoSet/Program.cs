﻿using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.DirectoryServices;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnixIdAutoSet
{
    class Program
    {
        static DateTime kUnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        static string kUnixObjectsGroup = "CN=Unix Objects,OU=Groups";
        static string kDefaultUnixUsersGroup = "CN=Unix Users Default Group,OU=Groups";
        static string kUserPrivateGroupContainer = "OU=Unix User Private Groups,OU=Groups";

        static bool commitChanges = true;

        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "dryrun")
            {
                commitChanges = false;
            }

            string directorySuffix = GetDirectorySuffix();

            using (var conn = new SQLiteConnection("Data Source=state.sqlite"))
            {
                conn.Open();
                CreateTables(conn);

                using (DirectoryEntry ldapConn = new DirectoryEntry())
                {
                    string userFilter = string.Format(
                        "(&(objectClass=user)(memberOf={0},{1})(!uidNumber=*))",
                        kUnixObjectsGroup,
                        directorySuffix
                    );
                    UpdateObjects(conn, ldapConn, userFilter, "uidNumber", "NextUserId");

                    string groupFilter = string.Format(
                        "(&(objectClass=group)(memberOf={0},{1})(!gidNumber=*))",
                        kUnixObjectsGroup,
                        directorySuffix
                    );
                    UpdateObjects(conn, ldapConn, groupFilter, "gidNumber", "NextGroupId");
                }

                UpdateUserPrivateGroups(conn, directorySuffix);
            }

            SetDefaultUserGid(directorySuffix);

            int x = 42;
        }

        static string GetDirectorySuffix()
        {
            using (DirectoryEntry ldapConn = new DirectoryEntry())
            {
                return (string)ldapConn.Properties["distinguishedName"].Value;
            }
        }

        static void UpdateObjects(SQLiteConnection sqlConn, DirectoryEntry root, string filter, string ldapAttribute, string nextIdSource)
        {
            using (DirectorySearcher search = new DirectorySearcher(root))
            {
                search.Filter = filter;
                search.PropertiesToLoad.Add("objectGUID");
                search.PropertiesToLoad.Add("distinguishedName");

                SearchResultCollection results = search.FindAll();
                int c = results.Count;

                foreach (SearchResult result in results)
                {
                    DirectoryEntry entry = result.GetDirectoryEntry();
                    int id = GetId(sqlConn, entry, nextIdSource);

                    string dn = (string)entry.Properties["distinguishedName"].Value;
                    Console.WriteLine(
                        "{3} id {0} for entry {1} ({2})",
                        id,
                        entry.Guid,
                        dn,
                        commitChanges ? "Setting" : "Would have set"
                    );

                    if (commitChanges)
                    {
                        entry.Properties[ldapAttribute].Value = id;
                        entry.CommitChanges();

                        Console.WriteLine("Set id {0} for entry {1} ({2})", id, entry.Guid, dn);
                    }
                }
            }
        }

        static void SetDefaultUserGid(string directorySuffix)
        {
            string ldapPath = string.Format(
                "LDAP://{0},{1}",
                kDefaultUnixUsersGroup,
                directorySuffix
            );

            DirectoryEntry defaultGroupEntry = new DirectoryEntry(ldapPath);
            int gid = (int)defaultGroupEntry.Properties["gidNumber"].Value;

            using (DirectoryEntry root = new DirectoryEntry())
            using (DirectorySearcher search = new DirectorySearcher(root))
            {
                string filter = string.Format(
                    "(&(objectClass=user)(memberOf={0},{1})(uidNumber=*)(!gidNumber=*))",
                    kUnixObjectsGroup,
                    directorySuffix
                );
                search.Filter = filter;

                SearchResultCollection results = search.FindAll();
                int c = results.Count;

                foreach (SearchResult result in results)
                {
                    DirectoryEntry entry = result.GetDirectoryEntry();
                    string dn = (string)entry.Properties["distinguishedName"].Value;

                    Console.WriteLine(
                        "{2} default gid for user {0} ({1})",
                        dn,
                        entry.Guid,
                        commitChanges ? "Setting" : "Would have set"
                    );

                    if (commitChanges)
                    {
                        entry.Properties["gidNumber"].Value = gid;
                        entry.CommitChanges();

                        Console.WriteLine("Set default gid for user {0} ({1})", dn, entry.Guid);
                    }
                }
            }
        }

        static void UpdateUserPrivateGroups(SQLiteConnection conn, string directorySuffix)
        {
            string ldapPath = string.Format(
                "LDAP://{0},{1}",
                kUserPrivateGroupContainer,
                directorySuffix
            );

            using (DirectoryEntry ldapConn = new DirectoryEntry(ldapPath))
            {
                UpdateObjects(conn, ldapConn, "(&(objectClass=group)(!gidNumber=*))", "gidNumber", "NextGroupId");
            }
        }

        static int GetId(SQLiteConnection conn, DirectoryEntry entry, string nextIdSource)
        {
            byte[] objectGuid = entry.Guid.ToByteArray();
            string dn = (string)entry.Properties["distinguishedName"].Value;

            using (var existingIdCommand = new SQLiteCommand(conn))
            {
                existingIdCommand.CommandText = @"SELECT AssignedId
FROM DirectoryEntry
WHERE ObjectGuid = @objectGuid;";

                existingIdCommand.Parameters.AddWithValue("@objectGuid", objectGuid);

                object value = existingIdCommand.ExecuteScalar();
                if (value != null)
                {
                    int existingId = (int)value;

                    Console.WriteLine("Got existing id {0} for entry {1} ({2})", existingId, entry.Guid, dn);

                    return existingId;
                }
            }

            using (var transaction = conn.BeginTransaction())
            {
                int nextId;
                using (var nextIdCommand = new SQLiteCommand(conn))
                {
                    nextIdCommand.CommandText = @"SELECT " + nextIdSource + @"
FROM NextId
WHERE Id = 1;";

                    nextId = (int)nextIdCommand.ExecuteScalar();
                }

                using (var assignIdCommand = new SQLiteCommand(conn))
                {
                    assignIdCommand.CommandText = @"INSERT INTO
DirectoryEntry(ObjectGuid, OriginalDistinguishedName, AssignedId, DateIdAssigned)
VALUES(@objectGuid, @distinguishedName, @assignedId, @dateIdAssigned);";

                    assignIdCommand.Parameters.AddWithValue("@objectGuid", objectGuid);
                    assignIdCommand.Parameters.AddWithValue("@assignedId", nextId);
                    assignIdCommand.Parameters.AddWithValue("@distinguishedName", dn);

                    long timestamp = (long)Math.Round((DateTime.UtcNow - kUnixEpoch).TotalMilliseconds);
                    assignIdCommand.Parameters.AddWithValue("@dateIdAssigned", timestamp);

                    assignIdCommand.ExecuteNonQuery();
                }

                int futureNextId = nextId + 1;
                using (var updateNextIdCommand = new SQLiteCommand(conn))
                {
                    updateNextIdCommand.CommandText = @"UPDATE NextId
SET " + nextIdSource + @" = @futureNextId
WHERE Id = 1 AND " + nextIdSource + @" = @nextId;";

                    updateNextIdCommand.Parameters.AddWithValue("@futureNextId", futureNextId);
                    updateNextIdCommand.Parameters.AddWithValue("@nextId", nextId);

                    int updatedRowCount = updateNextIdCommand.ExecuteNonQuery();
                    if (updatedRowCount < 1)
                    {
                        throw new Exception("Concurrency/consistency error.");
                    }
                    else
                    {
                        transaction.Commit();
                    }
                }

                Console.WriteLine("Generated id {0} for object {1} ({2})", nextId, entry.Guid, dn);

                return nextId;
            }
        }

        static void CreateTables(SQLiteConnection conn)
        {
            string nextIdCreateStatement = @"CREATE TABLE NextId
(
    Id INTEGER PRIMARY KEY NOT NULL,
    NextUserId INT NOT NULL,
    NextGroupId INT NOT NULL
);";

            string directoryEntryCreateStatement = @"CREATE TABLE DirectoryEntry
(
    ObjectGuid BLOB PRIMARY KEY NOT NULL,
    OriginalDistinguishedName TEXT NOT NULL,
    AssignedId INT NOT NULL,
    DateIdAssigned INT NOT NULL
);";
            if (CreateTable(conn, "NextId", nextIdCreateStatement))
            {
                string nextIdRowInsertStatement = @"INSERT INTO NextId(Id, NextUserId, NextGroupId) VALUES(1, 10000, 10000);";

                using (var cmd = new SQLiteCommand(nextIdRowInsertStatement, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            CreateTable(conn, "DirectoryEntry", directoryEntryCreateStatement);
        }

        static bool CreateTable(SQLiteConnection conn, string name, string createStatement)
        {
            using (var cmd = new SQLiteCommand(conn))
            {
                cmd.CommandText = @"SELECT *
FROM sqlite_master
WHERE type = 'table' AND name = @tableName;";

                cmd.Parameters.AddWithValue("@tableName", name);

                using (var dr = cmd.ExecuteReader())
                {
                    if (dr.Read())
                    {
                        return false;
                    }
                }
            }

            using (var cmd = new SQLiteCommand(createStatement, conn))
            {
                cmd.ExecuteNonQuery();
            }

            return true;
        }
    }
}
