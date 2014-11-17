using System.Collections.Generic;

namespace Pacifica.Core.Schema
{
    /// <summary>
    /// An arbitrary group of action and items.  Presumably the actions and items are 
    /// connected to each other in a tree or graph like structure.
    /// </summary>
    public class Group
    {
        public Group()
        {
            //TODO - a group may need to ensure actions and items
            //that get added are actually connected.
            Actions = new List<Action>();
            Items = new List<Item>();
        }

        public List<Action> Actions { get; private set; }
        public List<Item> Items { get; private set; }
    }
}
