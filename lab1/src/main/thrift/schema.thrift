 namespace java de.hska.iwi.bdelab.schema

union UserID {
  1: string user_id;
}

struct Website {
  1: string url;
}

struct PageViewEdge {
  1: required UserID user;
  2: required Website page;
  3: string time;
}

enum GenderType {
  MALE = 1,
  FEMALE = 2
}

union UserPropertyValue {
  1: string full_name;
  2: string email;
  3: GenderType gender;
}

struct UserProperty {
  1: required UserID id;
  2: required UserPropertyValue property;
}

struct FriendEdge {
  1: required UserID id1;
  2: required UserID id2;
}

struct Pedigree {
  1: required i32 true_as_of_secs;
}

union DataUnit {
  1: UserProperty user_property;
  2: FriendEdge friend;
  3: PageViewEdge page_view;
}

struct Data {
  1: required Pedigree pedigree;
  2: required DataUnit dataunit;
}
