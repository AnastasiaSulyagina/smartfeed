package parsing


object Relations {
  private val LOVE					            	= 1
  private val SPOUSE                      = 2
  private val PARENT                      = 3
  private val CHILD                       = 4
  private val BROTHER_SISTER              = 5
  private val UNCLE_AUNT                  = 6
  private val RELATIVE                    = 7
  private val CLOSE_FRIEND                = 8
  private val COLLEAGUE                   = 9
  private val SCHOOLMATE                  = 10
  private val NEPHEW                      = 11
  private val GRANDPARENT                 = 12
  private val GRANDCHILD                  = 13
  private val COLLEGE_UNIVERSITY_FELLOW   = 14
  private val ARMY_FELLOW                 = 15
  private val PARENT_IN_LAW               = 16
  private val CHILD_IN_LAW                = 17
  private val GODPARENT                   = 18
  private val GODCHILD                    = 19
  private val PLAYING_TOGETHER            = 20

  private val FAMILY_MASK =
      (1 << LOVE) | (1 << SPOUSE) | 
      (1 << PARENT) | (1 << CHILD) | 
      (1 << BROTHER_SISTER) | (1 << RELATIVE)
      (1 << UNCLE_AUNT) | (1 << NEPHEW) | 
      (1 << GRANDPARENT) | (1 << GRANDCHILD) | 
      (1 << PARENT_IN_LAW) | (1 << CHILD_IN_LAW) | 
      (1 << GODPARENT) | (1 << GODCHILD)

  private val COLLEAGUE_MASK = (1 << COLLEAGUE) | (1 << COLLEGE_UNIVERSITY_FELLOW)

  private val SCHOOLMATE_MASK = 1 << SCHOOLMATE

  private val ARMY_FELLOW_MASK = 1 << ARMY_FELLOW

  private val FRIEND_MASK = (1 << PLAYING_TOGETHER) | (1 << CLOSE_FRIEND)

  private def checkGroup(mask: Long, group: Long) =
    if ((mask & 0xFFFFFFFE & group) != 0) 1 else 0

  def isFamily(mask: Long) = checkGroup(mask, FAMILY_MASK)

  def isColleague(mask: Long) = checkGroup(mask, COLLEAGUE_MASK)

  def isSchoolmate(mask: Long) = checkGroup(mask, SCHOOLMATE_MASK)

  def isArmyFellow(mask: Long) = checkGroup(mask, ARMY_FELLOW_MASK)

  def isFriend(mask: Long) = checkGroup(mask, FRIEND_MASK)
}



