using System.Collections.Generic;

namespace Pacifica.Core.Schema
{
    public class Item
    {
        public Item()
        {
            Owners = new List<Action>();
        }

        public List<Action> Owners { get; private set; }
    }
}
